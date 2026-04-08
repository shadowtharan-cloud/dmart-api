require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const cors = require('cors');
const twilio = require('twilio');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// ─────────────────────────────────────────────────
// DATABASE
// ─────────────────────────────────────────────────
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// ─────────────────────────────────────────────────
// GEMINI KEY ROTATION
// ─────────────────────────────────────────────────
const GEMINI_KEYS = [];
for (let i = 1; i <= 20; i++) {
  const k = process.env['GEMINI_API_KEY_' + i];
  if (k && k.trim()) GEMINI_KEYS.push(k.trim());
}
if (process.env.GEMINI_API_KEY) GEMINI_KEYS.push(process.env.GEMINI_API_KEY);
if (GEMINI_KEYS.length === 0) { console.error('No Gemini keys!'); process.exit(1); }
console.log('Loaded ' + GEMINI_KEYS.length + ' Gemini key(s)');

let currentKeyIndex = 0;
const keyStatus = GEMINI_KEYS.map((_, i) => ({ index: i, calls: 0, exhausted: false, resetAt: null }));

function rotateKey() {
  for (let i = 1; i <= GEMINI_KEYS.length; i++) {
    const next = (currentKeyIndex + i) % GEMINI_KEYS.length;
    if (!keyStatus[next].exhausted) { currentKeyIndex = next; return true; }
  }
  keyStatus.forEach(k => { k.exhausted = false; k.resetAt = null; });
  currentKeyIndex = 0;
  return false;
}

setInterval(() => {
  const now = Date.now();
  keyStatus.forEach(k => { if (k.exhausted && k.resetAt && now > k.resetAt) { k.exhausted = false; } });
}, 60000);

// ONE FUNCTION — all Gemini calls go through here
async function callGemini(prompt) {
  let attempts = 0;
  while (attempts < GEMINI_KEYS.length) {
    try {
      const model = new GoogleGenerativeAI(GEMINI_KEYS[currentKeyIndex])
        .getGenerativeModel({ model: 'gemini-2.0-flash' });
      keyStatus[currentKeyIndex].calls++;
      const res = await model.generateContent(prompt);
      return res.response.text().trim();
    } catch (e) {
      const msg = e.message || '';
      if (msg.includes('429') || msg.includes('quota') || msg.includes('RESOURCE_EXHAUSTED')) {
        console.log('Key ' + (currentKeyIndex + 1) + ' quota hit — rotating');
        keyStatus[currentKeyIndex].exhausted = true;
        keyStatus[currentKeyIndex].resetAt = Date.now() + 65000;
        rotateKey();
        attempts++;
        await sleep(300);
        continue;
      }
      throw e;
    }
  }
  return null;
}

// ─────────────────────────────────────────────────
// TWILIO & STATE
// ─────────────────────────────────────────────────
const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const pendingFollowUp = new Map(); // phone -> timer handle
const pendingOrders = new Map();   // phone -> { orderId, productName, timer, stage }

// Conversation memory — remembers last shopping list result per customer
// So follow-up messages like "pack the available ones" work correctly
const conversationMemory = new Map(); // phone -> { lastIntent, availableItems, notAvailableItems, lastProducts, timestamp }

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────────────
// SETUP TABLES
// ─────────────────────────────────────────────────
async function ensureTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_interactions (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        phone_number VARCHAR(20),
        message TEXT,
        intent VARCHAR(50),
        category VARCHAR(50),
        bot_response TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS customer_orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        phone_number VARCHAR(20),
        product_name TEXT,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS order_bans (
        ban_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        phone_number VARCHAR(20),
        ban_until TIMESTAMP,
        reason TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
  } catch(e) { console.log('Table note:', e.message); }
}
ensureTables();

// ─────────────────────────────────────────────────
// SEND HELPERS
// ─────────────────────────────────────────────────
function formatPhone(raw) {
  if (raw.startsWith('whatsapp:')) return raw;
  const d = raw.replace(/[^0-9]/g, '');
  return `whatsapp:+${d.startsWith('91') ? d : '91' + d}`;
}

async function sendText(to, body) {
  try {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: formatPhone(to), body
    });
  } catch (e) { console.error('sendText error:', e.message); }
}

async function sendImage(to, imageUrl, caption) {
  try {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: formatPhone(to), body: caption, mediaUrl: [imageUrl]
    });
  } catch (e) {
    await sendText(to, caption); // fallback to text
  }
}

// ─────────────────────────────────────────────────
// DATABASE HELPERS
// ─────────────────────────────────────────────────
async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g, '').replace(/^91/, '');
  const r = await pool.query(`SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`, [`%${clean}%`]);
  return r.rows[0] || null;
}

async function isNewCustomer(id) {
  const r = await pool.query(`SELECT COUNT(*) as c FROM customer_interactions WHERE customer_id=$1`, [id]);
  return parseInt(r.rows[0].c) === 0;
}

async function getPreferences(id) {
  const r = await pool.query(
    `SELECT category, preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`, [id]
  );
  return r.rows;
}

// Search products by keyword — name, brand, or category
async function searchProductInDB(keyword) {
  const kw = `%${keyword}%`;
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
      AND (LOWER(p.name) LIKE LOWER($1) OR LOWER(p.brand) LIKE LOWER($1) OR LOWER(p.category) LIKE LOWER($1))
    ORDER BY COALESCE(o.discount_percent,0) DESC, p.price ASC
    LIMIT 5`, [kw]);
  return r.rows;
}

// Search multiple items at once with ONE query
async function searchMultipleItemsInDB(items) {
  if (!items || items.length === 0) return {};
  const results = {};
  // Build one query with OR conditions for all items
  const conditions = items.map((_, i) => `(LOWER(p.name) LIKE LOWER($${i+1}) OR LOWER(p.brand) LIKE LOWER($${i+1}) OR LOWER(p.category) LIKE LOWER($${i+1}))`).join(' OR ');
  const params = items.map(item => `%${item}%`);
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0 AND (${conditions})
    ORDER BY COALESCE(o.discount_percent,0) DESC`, params);

  // Map results back to each item
  for (const item of items) {
    const kl = item.toLowerCase();
    const match = r.rows.find(row =>
      row.name.toLowerCase().includes(kl) ||
      (row.brand && row.brand.toLowerCase().includes(kl)) ||
      row.category.toLowerCase().includes(kl)
    );
    results[item] = match || null;
  }
  return results;
}

async function getPreferredProducts(customerId, limit = 5) {
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
    ORDER BY cp.preference_score DESC, COALESCE(o.discount_percent,0) DESC
    LIMIT $2`, [customerId, limit]);
  return r.rows;
}

async function getOffers(customerId) {
  const r = await pool.query(`
    SELECT p.name, p.brand, p.price, p.image_url, o.discount_percent,
      ROUND(p.price*(1-o.discount_percent/100.0),2) as offer_price,
      ROUND(p.price*o.discount_percent/100.0,2) as you_save,
      p.category, COALESCE(cp.preference_score,0) as relevance
    FROM offers o
    JOIN products p ON p.product_id=o.product_id
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    WHERE o.valid_till>NOW() AND p.is_available=true AND p.stock_quantity>0
    ORDER BY relevance DESC, o.discount_percent DESC LIMIT 6`, [customerId]);
  return r.rows;
}

async function addProductToDB(name, category, brand, price) {
  try {
    await pool.query(`
      INSERT INTO products(name,category,brand,price,cost_price,stock_quantity,reorder_threshold,is_available,image_url)
      VALUES($1,$2,$3,$4,$5,100,10,true,'') ON CONFLICT DO NOTHING`,
      [name, category, brand || '', price, Math.round(price * 0.7)]
    );
  } catch (e) { console.log('addProduct note:', e.message); }
}

async function logInteraction(customerId, phone, message, intent, category, reply) {
  try {
    await pool.query(
      `INSERT INTO customer_interactions(customer_id,phone_number,message,intent,category,bot_response) VALUES($1,$2,$3,$4,$5,$6)`,
      [customerId, phone, message, intent, category, reply]
    );
  } catch (e) {}
}

// ─────────────────────────────────────────────────
// THE ONLY GEMINI CALL — does everything in one shot
// Intent + reply + items + product checks — ALL in one call
// ─────────────────────────────────────────────────
async function analyzeMessage(message, customerName, prefs, unknownItems = [], context = null) {
  const prefList = prefs.map(p => p.category).join(', ') || 'General';
  const unknownSection = unknownItems.length > 0 ? `
Also check these NOT in our database — tell if Dmart India sells it and approximate price:
Items: ${unknownItems.join(', ')}
Include under "gemini_products": [{name, category, brand, price, available_at_dmart: true/false}]` : '';

  const contextSection = context ? `
PREVIOUS MESSAGE CONTEXT:
- Last action: ${context.lastIntent}
- Available items from last list: ${(context.availableItems||[]).join(', ')||'none'}
- Not available: ${(context.notAvailableItems||[]).join(', ')||'none'}
- Last products shown: ${(context.lastProducts||[]).slice(0,3).map(p=>p.name).join(', ')||'none'}
If the new message is a follow-up to above (like "pack available ones", "order available items", "show those products", "except unavailable show available") — set intent to "follow_up_action" and follow_up_type to "order_available" or "show_available".` : '';

  const prompt = `You are Dmart India WhatsApp shopping assistant.
Customer: ${customerName} | Preferences: ${prefList}
Message: "${message}"
${contextSection}
${unknownSection}
Reply ONLY valid JSON no markdown:
{
  "intent": "greeting|shopping_list|search_product|browse_category|check_offers|place_order|follow_up_action|question|out_of_scope",
  "follow_up_type": null,
  "items": [],
  "keyword": "main product/category",
  "order_product": null,
  "is_dmart_related": true,
  "reply": "friendly 1-2 sentence reply, subtly mention Dmart saves money vs delivery apps without naming them",
  "dmart_answer": null,
  "gemini_products": []
}
Intent: follow_up_action=customer responds to previous list result | shopping_list=3+ items to check | place_order=specific product order | search_product=1-2 products | browse_category=show category | check_offers=deals | question=store info | greeting=hello | out_of_scope=not shopping
For question: put 2-sentence answer in dmart_answer.
Categories: Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household|Grains|Spices|Cleaning|Footwear`;

  try {
    const raw = await callGemini(prompt);
    if (!raw) throw new Error('No response');
    const parsed = JSON.parse(raw.replace(/```json|```/g, '').trim());
    if (!parsed.items) parsed.items = [];
    if (!parsed.gemini_products) parsed.gemini_products = [];
    if (!parsed.reply) parsed.reply = `On it ${customerName}! 😊`;
    return parsed;
  } catch(e) {
    console.log('analyzeMessage error:', e.message);
    return { intent:'search_product', follow_up_type:null, items:[], keyword:message.split(' ').filter(w=>w.length>2).slice(0,2).join(' ')||message, order_product:null, is_dmart_related:true, reply:`On it ${customerName}! 😊`, dmart_answer:null, gemini_products:[] };
  }
}

// ─────────────────────────────────────────────────
// PRODUCT CARD
// ─────────────────────────────────────────────────
async function sendProductCard(to, product, isFirst) {
  let cap = '';
  if (isFirst) cap += `⭐ *TOP PICK*\n`;
  cap += `*${product.name}*`;
  if (product.brand) cap += ` — ${product.brand}`;
  cap += `\n📦 ${product.category}\n`;

  const discount = parseFloat(product.discount_percent || 0);
  if (discount > 0) {
    const final = product.final_price || product.offer_price || product.price;
    const save = (parseFloat(product.price) - parseFloat(final)).toFixed(0);
    cap += `~~Rs.${product.price}~~ → *Rs.${final}* 🏷️ *${discount}% OFF*\n`;
    cap += `💰 You save Rs.${save}`;
  } else {
    cap += `💵 *Rs.${product.price}*`;
  }
  cap += `\n\n📍 Available at Dmart — grab it fresh!`;

  if (product.image_url && product.image_url.startsWith('http')) {
    await sendImage(to, product.image_url, cap);
  } else {
    await sendText(to, cap);
  }
}

// ─────────────────────────────────────────────────
// WELCOME MESSAGE
// ─────────────────────────────────────────────────
function buildWelcome(name) {
  return `👋 *Hey ${name}! Welcome to Dmart Assistant!* 🛒

I'm your personal shopping friend at Dmart. Here's what I can do for you:

🔍 *Find products* — Just ask! "Show me snacks" or "Do you have Lays?"
📋 *Check your shopping list* — Send your full list and I'll check what's available with prices
🏷️ *Show today's offers* — Ask "What deals today?"
📦 *Note your order* — Say "Order it, I'll pick it up"
💡 *Answer questions* — Store timings, return policy, Dmart app — anything!

I know what you like and I'll always find you the best deals. 😊

*What can I help you with today?*`;
}

// ─────────────────────────────────────────────────
// FOLLOW UP — sent after 60s of no reply
// Uses DB only — NO Gemini call
// ─────────────────────────────────────────────────
async function sendFollowUp(to, customer) {
  const prods = await getPreferredProducts(customer.customer_id, 3);
  if (prods.length === 0) {
    await sendText(to,
      `👋 *${customer.name}!* Just checking in — anything you need from Dmart today?\n\n` +
      `Send your shopping list or just ask for any product! 😊`
    );
    return;
  }

  const topWithOffer = prods.find(p => parseFloat(p.discount_percent) > 0);
  const highlight = topWithOffer || prods[0];

  await sendText(to,
    `🛒 *${customer.name}, I know what you love at Dmart!*\n\n` +
    `Right now *${highlight.name}* ${parseFloat(highlight.discount_percent) > 0
      ? `is at *${highlight.discount_percent}% OFF* — Rs.${highlight.final_price} instead of Rs.${highlight.price}!`
      : `is in stock at Rs.${highlight.price}.`}\n\n` +
    `These are fresh and waiting for you — no delivery wait, no hidden fees. Just walk in! 🚶‍♂️\n\n` +
    `Want to see your full picks or check an offer? Just ask! 😊`
  );

  await sleep(600);

  // Send top 2 product cards — no Gemini needed
  for (let i = 0; i < Math.min(prods.length, 2); i++) {
    await sendProductCard(to, prods[i], i === 0);
    await sleep(500);
  }
}

// ─────────────────────────────────────────────────
// SHOPPING LIST HANDLER
// Key optimization: ONE DB query for all items, ONE Gemini call for all unknowns
// ─────────────────────────────────────────────────
async function handleShoppingList(to, customer, items, prefs, parsedReply) {
  await sendText(to,
    `${parsedReply}\n\n📋 *Checking your ${items.length} items at Dmart...*`
  );

  // Step 1 — check ALL items in DB with ONE query
  const dbResults = await searchMultipleItemsInDB(items);

  const foundItems = [];
  const unknownItems = [];

  for (const item of items) {
    if (dbResults[item]) {
      foundItems.push({ item, product: dbResults[item] });
    } else {
      unknownItems.push(item);
    }
  }

  // Step 2 — if there are unknowns, check ALL with ONE Gemini call (not one per item!)
  let geminiProducts = {};
  if (unknownItems.length > 0) {
    console.log('Checking unknowns with Gemini (1 call for all):', unknownItems);
    const analysis = await analyzeMessage(
      `Check availability: ${unknownItems.join(', ')}`,
      customer.name, prefs, unknownItems
    );
    // Map gemini results
    for (const gp of (analysis.gemini_products || [])) {
      const matchItem = unknownItems.find(i =>
        gp.name.toLowerCase().includes(i.toLowerCase()) ||
        i.toLowerCase().includes(gp.name.toLowerCase().split(' ')[0])
      );
      if (matchItem) geminiProducts[matchItem] = gp;
    }
  }

  // Step 3 — Build the full availability report
  await sleep(500);

  const available = [];
  const notAvailable = [];

  // Items found in DB
  for (const { item, product } of foundItems) {
    const discount = parseFloat(product.discount_percent || 0);
    let line = `✅ *${item}* — Rs.${discount > 0 ? product.final_price : product.price}`;
    if (discount > 0) line += ` *(${discount}% OFF!)*`;
    available.push(line);
  }

  // Unknown items — use Gemini result
  for (const item of unknownItems) {
    const gp = geminiProducts[item];
    if (gp && gp.available_at_dmart) {
      // Add to DB silently
      addProductToDB(gp.name, gp.category, gp.brand || '', gp.price);
      available.push(`✅ *${item}* — Rs.${gp.price} (Dmart price)`);
    } else {
      notAvailable.push(`❌ *${item}* — not available at Dmart`);
    }
  }

  // Send the report
  let report = `📋 *Shopping List Result for ${customer.name}:*\n\n`;
  if (available.length > 0) {
    report += `*Available at Dmart (${available.length}/${items.length}):*\n`;
    report += available.join('\n');
  }
  if (notAvailable.length > 0) {
    report += `\n\n*Not available (${notAvailable.length}):*\n`;
    report += notAvailable.join('\n');
  }

  await sendText(to, report);
  await sleep(700);

  // Send product cards only for DB items with images (max 3)
  const withImages = foundItems.filter(f => f.product.image_url && f.product.image_url.startsWith('http'));
  for (let i = 0; i < Math.min(withImages.length, 3); i++) {
    await sendProductCard(to, withImages[i].product, i === 0);
    await sleep(600);
  }

  const totalSavings = foundItems.reduce((acc, f) => {
    const d = parseFloat(f.product.discount_percent || 0);
    if (d > 0) acc += parseFloat(f.product.price) - parseFloat(f.product.final_price);
    return acc;
  }, 0);

  await sendText(to,
    `🛒 *${available.length} items ready at Dmart!*` +
    (totalSavings > 0 ? ` You save Rs.${totalSavings.toFixed(0)} with active offers!` : '') +
    `\n\nJust walk in — everything's in stock. No delivery wait, no extra charges. 😊\n\n` +
    `Want to order and note it for pickup? Just say *"pack it, I'll pick it up"*! 📦`
  );

  // SAVE CONVERSATION MEMORY — so follow-up messages work
  const phone = to.replace('whatsapp:', '').replace('+', '');
  const availableNames = [...foundItems.map(f => f.item), ...unknownItems.filter(i => geminiProducts[i]?.available_at_dmart).map(i => geminiProducts[i].name)];
  const notAvailableNames = unknownItems.filter(i => !geminiProducts[i]?.available_at_dmart);
  conversationMemory.set(phone, {
    lastIntent: 'shopping_list',
    availableItems: availableNames,
    notAvailableItems: notAvailableNames,
    lastProducts: foundItems.map(f => f.product),
    timestamp: Date.now()
  });
  // Memory expires after 30 minutes
  setTimeout(() => conversationMemory.delete(phone), 30 * 60 * 1000);
}

// ─────────────────────────────────────────────────
// ORDER SYSTEM
// ─────────────────────────────────────────────────
async function isCustomerBanned(customerId, phone) {
  try {
    const r = await pool.query(
      `SELECT * FROM order_bans WHERE (customer_id=$1 OR phone_number=$2) AND ban_until > NOW() LIMIT 1`,
      [customerId, phone]
    );
    return r.rows[0] || null;
  } catch(e) { return null; }
}

async function getCancelledCount(customerId) {
  try {
    const r = await pool.query(
      `SELECT COUNT(*) as c FROM customer_orders WHERE customer_id=$1 AND status='cancelled' AND created_at > NOW() - INTERVAL '30 days'`,
      [customerId]
    );
    return parseInt(r.rows[0].c);
  } catch(e) { return 0; }
}

async function handleOrder(to, customer, productName) {
  const phone = to.replace('whatsapp:', '').replace('+', '');

  const ban = await isCustomerBanned(customer.customer_id, phone);
  if (ban) {
    const banDate = new Date(ban.ban_until).toLocaleDateString('en-IN');
    await sendText(to,
      `⚠️ *${customer.name}, ordering is paused until ${banDate}.*\n\n` +
      `Due to previous uncollected orders, this feature is paused. You can still browse products! 🛒`
    );
    return;
  }

  const cancelCount = await getCancelledCount(customer.customer_id);
  if (cancelCount >= 3) {
    try {
      await pool.query(
        `INSERT INTO order_bans(customer_id,phone_number,ban_until,reason) VALUES($1,$2,NOW() + INTERVAL '30 days',$3)`,
        [customer.customer_id, phone, 'Too many cancelled orders']
      );
    } catch(e) {}
    await sendText(to,
      `⚠️ *${customer.name}, ordering has been paused for 30 days.*\n\n` +
      `3 orders were placed but not picked up. To keep things fair, ordering is paused.\n` +
      `You can still browse and check availability! 😊`
    );
    return;
  }

  // Create order
  let orderId;
  try {
    const r = await pool.query(
      `INSERT INTO customer_orders(customer_id,phone_number,product_name,status) VALUES($1,$2,$3,'pending') RETURNING order_id`,
      [customer.customer_id, phone, productName]
    );
    orderId = r.rows[0].order_id;
  } catch(e) { orderId = null; }

  await sendText(to,
    `✅ *Got it ${customer.name}!*\n\n` +
    `Your order for *${productName}* is noted.\n` +
    `⏳ Give us *3 minutes* to prepare it!\n\n` +
    `📍 Please have payment ready when you come to Dmart. _(Pay at the counter — easy!)_ 😊`
  );

  // 3-minute ready timer
  const readyTimer = setTimeout(async () => {
    try {
      if (orderId) await pool.query(`UPDATE customer_orders SET status='ready', updated_at=NOW() WHERE order_id=$1`, [orderId]);
      await sendText(to,
        `🎉 *${customer.name}, your order is READY!*\n\n` +
        `✅ *${productName}* is packed and waiting at Dmart!\n` +
        `📍 Come pick it up anytime. Payment at counter when you arrive. 😊`
      );

      // 30-minute no-show timer
      const noShowTimer = setTimeout(async () => {
        try {
          if (orderId) await pool.query(`UPDATE customer_orders SET status='cancelled', updated_at=NOW() WHERE order_id=$1`, [orderId]);
          const count = await getCancelledCount(customer.customer_id);
          await sendText(to,
            `⚠️ *Order cancelled — ${customer.name}*\n\n` +
            `We waited but you didn't come, so *${productName}* order is cancelled.\n\n` +
            `No worries, you can order again anytime! 😊\n` +
            `_(Note: ${count}/3 cancellations — after 3, ordering is paused for 1 month.)_`
          );
          pendingOrders.delete(phone);
        } catch(e) { console.error('no-show timer:', e.message); }
      }, 30 * 60 * 1000);

      pendingOrders.set(phone, { orderId, productName, timer: noShowTimer, stage: 'ready' });
    } catch(e) { console.error('ready timer:', e.message); }
  }, 3 * 60 * 1000);

  pendingOrders.set(phone, { orderId, productName, timer: readyTimer, stage: 'pending' });
}

// ─────────────────────────────────────────────────
// MAIN MESSAGE PROCESSOR
// Max 1 Gemini call per message (sometimes 0 for follow-ups)
// ─────────────────────────────────────────────────
async function processMessage(fromRaw, message) {
  console.log('\n═══ MESSAGE ═══');
  console.log('From:', fromRaw, '| Body:', message);

  const phone = fromRaw.replace('whatsapp:', '').replace('+', '');

  // Cancel pending follow-up — they replied
  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
  }

  // Get customer
  const customer = await getCustomerByPhone(phone);
  if (!customer) {
    await sendText(fromRaw,
      `👋 *Welcome to Dmart Assistant!*\n\n` +
      `Your number isn't registered yet. Visit Dmart to register and unlock personalized shopping!\n\n` +
      `You can still ask about any product. 🛒`
    );
    return;
  }

  console.log('Customer:', customer.name);
  const prefs = await getPreferences(customer.customer_id);

  // First-ever message — welcome + schedule follow-up (0 Gemini calls)
  const isNew = await isNewCustomer(customer.customer_id);
  if (isNew) {
    await sendText(fromRaw, buildWelcome(customer.name));
    await logInteraction(customer.customer_id, phone, message, 'welcome', null, 'welcome sent');

    const handle = setTimeout(async () => {
      try {
        await sendFollowUp(fromRaw, customer);
        pendingFollowUp.delete(phone);
      } catch(e) { console.error('follow-up error:', e.message); }
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // ── PARSE WITH GEMINI (1 call) ──
  // Pass conversation memory so Gemini understands follow-up messages
  const context = conversationMemory.get(phone) || null;
  const parsed = await analyzeMessage(message, customer.name, prefs, [], context);
  console.log('Intent:', parsed.intent, '| follow_up_type:', parsed.follow_up_type, '| Keyword:', parsed.keyword);

  // ── FOLLOW-UP ACTION (e.g. "pack the available ones", "show me the available products") ──
  if (parsed.intent === 'follow_up_action' && context) {
    const { availableItems, lastProducts } = context;

    if (!availableItems || availableItems.length === 0) {
      await sendText(fromRaw,
        `😊 ${parsed.reply}\n\nHmm, I don't have a recent shopping list result to refer to. Could you send your list again? 📋`
      );
      return;
    }

    if (parsed.follow_up_type === 'order_available') {
      // Customer wants to order all available items
      await sendText(fromRaw,
        `✅ *Got it ${customer.name}!*\n\n` +
        `I'm noting your order for these available items:\n` +
        availableItems.map(i => `• ${i}`).join('\n') +
        `\n\n⏳ Give us *3 minutes* to prepare everything!\n\n` +
        `📍 Please have payment ready when you come to Dmart. _(Pay at the counter — easy!)_ 😊`
      );

      // Create orders for each item
      for (const item of availableItems.slice(0, 5)) {
        try {
          await pool.query(
            `INSERT INTO customer_orders(customer_id,phone_number,product_name,status) VALUES($1,$2,$3,'pending')`,
            [customer.customer_id, phone, item]
          );
        } catch(e) {}
      }

      setTimeout(async () => {
        try {
          await sendText(fromRaw,
            `🎉 *${customer.name}, your items are READY!*\n\n` +
            `✅ Packed and waiting at Dmart:\n` +
            availableItems.map(i => `• ${i}`).join('\n') +
            `\n\n📍 Come pick them up anytime. Payment at the counter! 😊`
          );
        } catch(e) {}
      }, 3 * 60 * 1000);

    } else {
      // Customer wants to see the available products again
      await sendText(fromRaw,
        `${parsed.reply}\n\n✅ *Here are the available items from your list:*\n\n` +
        availableItems.map(i => `• ${i}`).join('\n') +
        `\n\nSay *"pack it, I'll pick it up"* to order all of these! 📦`
      );
      await sleep(700);
      // Show product cards for items we have images for
      const cardsToShow = lastProducts?.filter(p => p.image_url && p.image_url.startsWith('http')).slice(0, 3) || [];
      for (let i = 0; i < cardsToShow.length; i++) {
        await sendProductCard(fromRaw, cardsToShow[i], i === 0);
        await sleep(600);
      }
    }

    await logInteraction(customer.customer_id, phone, message, 'follow_up_action', null, 'follow-up handled');
    return;
  }

  // ── GREETING — no DB search needed ──
  if (parsed.intent === 'greeting') {
    const offers = await getOffers(customer.customer_id);
    const hasOffers = offers.length > 0;
    await sendText(fromRaw,
      `${parsed.reply}\n\n` +
      (hasOffers
        ? `🔥 By the way — *${offers.length} deals* are live right now, some on products you love! Ask me *"show offers"* to see them.\n\n`
        : '') +
      `What do you need today? Send your list or ask for any product! 😊`
    );
    await logInteraction(customer.customer_id, phone, message, 'greeting', null, 'greeted');
    return;
  }

  // ── OUT OF SCOPE ──
  if (!parsed.is_dmart_related || parsed.intent === 'out_of_scope') {
    await sendText(fromRaw,
      `😄 Ha! I wish I could help with that ${customer.name}!\n\n` +
      `But I'm your *Dmart shopping friend* — only good at products and deals! 🛒\n\n` +
      `Try: *"Show snacks"* or *"What offers today?"* or just send your shopping list!`
    );
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, 'redirected');

    const handle = setTimeout(async () => {
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch(e) {}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // ── DMART QUESTION — answer already in parsed.dmart_answer ──
  if (parsed.intent === 'question') {
    const answer = parsed.dmart_answer || `I'd recommend checking with your nearest Dmart store for the most accurate info! 😊`;
    await sendText(fromRaw, `💡 ${answer}\n\nAnything else I can help with? 😊`);
    await logInteraction(customer.customer_id, phone, message, 'question', null, answer);
    return;
  }

  // ── SHOPPING LIST ──
  if (parsed.intent === 'shopping_list' && parsed.items && parsed.items.length >= 2) {
    await handleShoppingList(fromRaw, customer, parsed.items, prefs, parsed.reply);
    await logInteraction(customer.customer_id, phone, message, 'shopping_list', null, `checked ${parsed.items.length} items`);
    return;
  }

  // ── PLACE ORDER ──
  if (parsed.intent === 'place_order') {
    const productName = parsed.order_product || parsed.keyword || 'your item';
    if (pendingOrders.has(phone)) {
      clearTimeout(pendingOrders.get(phone).timer);
      pendingOrders.delete(phone);
    }
    await handleOrder(fromRaw, customer, productName);
    await logInteraction(customer.customer_id, phone, message, 'place_order', productName, 'order placed');
    return;
  }

  // ── CHECK OFFERS ──
  if (parsed.intent === 'check_offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length === 0) {
      await sendText(fromRaw, `No active offers right now ${customer.name}! Want to browse products instead? 😊`);
      return;
    }
    await sendText(fromRaw,
      `${parsed.reply}\n\n🔥 *${offers.length} live deals — including on things YOU like!*`
    );
    await sleep(600);
    for (let i = 0; i < Math.min(offers.length, 5); i++) {
      const o = offers[i];
      const cap =
        `🏷️ *${o.name}*${o.brand ? ' — ' + o.brand : ''}\n` +
        `~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n` +
        `💰 You save Rs.${o.you_save}!\n\n` +
        `📍 In stock at Dmart — grab it yourself and keep that extra money! 😊`;
      if (o.image_url && o.image_url.startsWith('http')) {
        await sendImage(fromRaw, o.image_url, cap);
      } else {
        await sendText(fromRaw, cap);
      }
      await sleep(600);
    }
    await sendText(fromRaw, `Want to check your shopping list? Just paste it and I'll check everything! 📋`);
    await logInteraction(customer.customer_id, phone, message, 'check_offers', null, 'offers shown');
    return;
  }

  // ── SEARCH PRODUCT or BROWSE CATEGORY ──
  const keyword = parsed.keyword || message.split(' ').filter(w => w.length > 2).slice(0, 2).join(' ');
  let dbResults = await searchProductInDB(keyword);

  if (dbResults.length > 0) {
    await sendText(fromRaw,
      `${parsed.reply}\n\n🛒 *Found ${dbResults.length} match${dbResults.length > 1 ? 'es' : ''} for "${keyword}" at Dmart:*`
    );
    await sleep(600);
    for (let i = 0; i < Math.min(dbResults.length, 4); i++) {
      await sendProductCard(fromRaw, dbResults[i], i === 0);
      await sleep(600);
    }
    const offersCount = dbResults.filter(p => parseFloat(p.discount_percent) > 0).length;
    await sendText(fromRaw,
      (offersCount > 0 ? `🎉 *${offersCount} of these have active offers!*\n\n` : '') +
      `📍 All in stock — just walk in and grab it. No delivery wait, no extra fees! 🛒\n\n` +
      `Need more? Send your full shopping list and I'll check everything! 📋`
    );
  } else {
    // Not in DB — Gemini already ran once. Re-use parsed.gemini_products if available
    // OR make a targeted check (still 0 extra calls if gemini_products has result)
    const gps = parsed.gemini_products || [];
    const found = gps.find(gp =>
      gp.name.toLowerCase().includes(keyword.toLowerCase()) ||
      keyword.toLowerCase().includes(gp.name.toLowerCase().split(' ')[0])
    );

    if (found && found.available_at_dmart) {
      addProductToDB(found.name, found.category, found.brand || '', found.price);
      await sendText(fromRaw,
        `${parsed.reply}\n\n✅ *Found "${found.name}" at Dmart!*\n\n` +
        `📦 ${found.category}${found.brand ? ` — ${found.brand}` : ''}\n` +
        `💵 *Rs.${found.price}* (Dmart price)\n\n` +
        `📍 Available at Dmart — pick it up yourself and save on delivery! 😊\n\n` +
        `Reply *"order ${found.name}"* to note your order! 🛒`
      );
    } else {
      // Last resort — check with a dedicated Gemini call
      console.log('Making targeted product check for:', keyword);
      const checkPrompt = `Is "${keyword}" sold at Dmart India stores? Reply ONLY JSON:
{"available_at_dmart":true/false,"name":"exact name","category":"category","brand":"brand or empty","price":price_number}
Dmart sells groceries, FMCG, clothing, footwear, home goods — not electronics or medicines.`;
      let gResult = { available_at_dmart: false };
      try {
        const raw = await callGemini(checkPrompt);
        if (raw) gResult = JSON.parse(raw.replace(/```json|```/g, '').trim());
      } catch(e) {}

      if (gResult.available_at_dmart) {
        addProductToDB(gResult.name, gResult.category, gResult.brand || '', gResult.price);
        await sendText(fromRaw,
          `${parsed.reply}\n\n✅ *"${gResult.name}" is available at Dmart!*\n\n` +
          `📦 ${gResult.category}${gResult.brand ? ` — ${gResult.brand}` : ''}\n` +
          `💵 *Rs.${gResult.price}*\n\n` +
          `📍 Pick it up at Dmart — save on delivery! 😊`
        );
      } else {
        await sendText(fromRaw,
          `${parsed.reply}\n\n😕 *"${keyword}" doesn't seem to be at Dmart.*\n\n` +
          `Want me to show similar products? Try:\n` +
          `🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🌾 Grains`
        );
      }
    }
  }

  await logInteraction(customer.customer_id, phone, message, parsed.intent, keyword, 'replied');
}

// ─────────────────────────────────────────────────
// ENDPOINTS
// ─────────────────────────────────────────────────
app.post('/whatsapp', (req, res) => {
  console.log('\n─── WEBHOOK ───', 'From:', req.body.From, '| Msg:', req.body.Body);
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>'); // instant reply to Twilio

  const fromRaw = req.body.From || '';
  const message = (req.body.Body || '').trim();
  if (fromRaw && message) {
    processMessage(fromRaw, message).catch(e => console.error('processMessage error:', e.message));
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

app.get('/keystatus', (req, res) => res.json({
  total_keys: GEMINI_KEYS.length,
  active_key: currentKeyIndex + 1,
  keys: keyStatus.map(k => ({
    key: k.index + 1,
    calls: k.calls,
    exhausted: k.exhausted,
    resets_at: k.resetAt ? new Date(k.resetAt).toISOString() : null
  }))
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`\n✅ Dmart Assistant running on port ${PORT}\n`));
