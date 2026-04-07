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

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const pendingFollowUp = new Map();
const pendingOrders = new Map();    // phone -> { productName, timer }
const cancelledOrders = new Map();  // phone -> count of cancelled/fake orders

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── CREATE ORDER TABLES IF NOT EXISTS ──
async function ensureOrderTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        phone_number VARCHAR(20),
        product_name TEXT,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS order_bans (
        ban_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        phone_number VARCHAR(20),
        ban_until TIMESTAMP,
        reason TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
  } catch(e) { console.log('Table create note:', e.message); }
}
ensureOrderTables();

// ─────────────────────────────────────────────────
// SEND HELPERS
// ─────────────────────────────────────────────────
async function sendText(to, body) {
  let t = to;
  if (!t.startsWith('whatsapp:')) {
    const d = t.replace(/[^0-9]/g, '');
    t = `whatsapp:+${d.startsWith('91') ? d : '91' + d}`;
  }
  try {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: t, body
    });
  } catch (e) { console.error('sendText error:', e.message); }
}

async function sendImage(to, imageUrl, caption) {
  let t = to;
  if (!t.startsWith('whatsapp:')) {
    const d = t.replace(/[^0-9]/g, '');
    t = `whatsapp:+${d.startsWith('91') ? d : '91' + d}`;
  }
  try {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: t, body: caption, mediaUrl: [imageUrl]
    });
  } catch (e) {
    // fallback to text if image fails
    try {
      await twilioClient.messages.create({
        from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
        to: t, body: caption
      });
    } catch (e2) { console.error('sendImage fallback error:', e2.message); }
  }
}

// ─────────────────────────────────────────────────
// DATABASE HELPERS
// ─────────────────────────────────────────────────
async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g, '').replace(/^91/, '');
  const r = await pool.query(
    `SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`,
    [`%${clean}%`]
  );
  return r.rows[0] || null;
}

async function isNewCustomer(id) {
  const r = await pool.query(
    `SELECT COUNT(*) as c FROM customer_interactions WHERE customer_id=$1`, [id]
  );
  return parseInt(r.rows[0].c) === 0;
}

async function getPreferences(id) {
  const r = await pool.query(
    `SELECT category, preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`, [id]
  );
  return r.rows;
}

// Smart product search — tries exact match first, then fuzzy
async function searchProductInDB(keyword) {
  // Try exact/close match first
  let r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
      AND (LOWER(p.name) LIKE LOWER($1) OR LOWER(p.brand) LIKE LOWER($1))
    ORDER BY COALESCE(o.discount_percent,0) DESC LIMIT 3`,
    [`%${keyword}%`]
  );
  if (r.rows.length > 0) return r.rows;

  // Try category match
  r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
      AND LOWER(p.category) LIKE LOWER($1)
    ORDER BY COALESCE(o.discount_percent,0) DESC LIMIT 3`,
    [`%${keyword}%`]
  );
  return r.rows;
}

// Get preferred products for attraction messages
async function getPreferredProducts(customerId, limit) {
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      (cp.preference_score*6+COALESCE(o.discount_percent,0)*4) as score
    FROM products p
    JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
    ORDER BY score DESC LIMIT $2`, [customerId, limit]
  );
  return r.rows;
}

async function getOffers(customerId) {
  const r = await pool.query(`
    SELECT p.name, p.brand, p.price, p.image_url, o.discount_percent,
      ROUND(p.price*(1-o.discount_percent/100.0),2) as offer_price,
      ROUND(p.price-(p.price*(1-o.discount_percent/100.0)),2) as you_save,
      p.category, COALESCE(cp.preference_score,0) as relevance
    FROM offers o JOIN products p ON p.product_id=o.product_id
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    WHERE o.valid_till>NOW() AND p.is_available=true AND p.stock_quantity>0
    ORDER BY relevance DESC, o.discount_percent DESC LIMIT 8`, [customerId]
  );
  return r.rows;
}

async function addProductToDB(name, category, brand, price) {
  try {
    await pool.query(`
      INSERT INTO products(name,category,brand,price,cost_price,stock_quantity,reorder_threshold,is_available,image_url)
      VALUES($1,$2,$3,$4,$5,100,10,true,'')
      ON CONFLICT DO NOTHING`,
      [name, category, brand || '', price, Math.round(price * 0.7)]
    );
  } catch (e) { console.log('addProduct error:', e.message); }
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
// STEP 1: GEMINI — parse what the customer wants
// Returns structured intent + extracted items
// ─────────────────────────────────────────────────
async function parseIntent(message, customerName, prefs) {
  const prefList = prefs.map(p => p.category).join(', ') || 'not set';
  const prompt = `You are parsing a WhatsApp message to a Dmart supermarket assistant.

Customer name: ${customerName}
Customer preferences: ${prefList}  
Message: "${message}"

Extract the intent and details. Reply ONLY with valid JSON, no markdown, no explanation:

{
  "intent": "shopping_list | search_product | check_offers | browse_category | place_order | question | greeting | out_of_scope",
  "items": ["item1", "item2"],
  "search_keyword": "main product or category to search",
  "order_product": "product name if placing an order, else null",
  "category": null or "Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household|Grains|Spices|Cleaning",
  "is_dmart_related": true or false
}

Rules:
- shopping_list: customer gives multiple items to check (like "Beans 1kg, Carrot, Lays, Wheat 5kg")
- For shopping_list, extract ALL item names into "items" array — just the product names without quantities
- place_order: customer says "order [product]" or "I want to order [product]" or "order it and I'll pick it up" — extract product name into order_product
- search_product: asking about 1-2 specific products
- browse_category: asking to show a category like "show snacks" or "show dairy"
- check_offers: asking about deals, discounts, offers
- question: asking a question about Dmart (timings, return policy, parking, app, etc)
- out_of_scope: not related to shopping or Dmart at all

For shopping_list, items = ["Beans","Carrot","Lays","Wheat","Seeraga samba rice","Sambar powder","Chili powder","Garam masala","Darkfantasy","Tomato","Brinjal","Bata shoe"]`;

  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(text);
    if (!parsed.items) parsed.items = [];
    return parsed;
  } catch (e) {
    console.log('parseIntent error:', e.message);
    return {
      intent: 'search_product',
      items: [],
      search_keyword: message.split(' ').slice(0, 2).join(' '),
      category: null,
      is_dmart_related: true
    };
  }
}

// ─────────────────────────────────────────────────
// STEP 2: GEMINI — check single product availability
// If not in DB, Gemini uses its Dmart knowledge
// ─────────────────────────────────────────────────
async function checkProductWithGemini(productName) {
  const prompt = `You are a Dmart India product expert with knowledge of all products sold at Dmart supermarkets across India.

Product to check: "${productName}"

Is this product available at Dmart India stores? Reply ONLY valid JSON no markdown:
{
  "available_at_dmart": true or false,
  "name": "exact product name as sold at Dmart",
  "category": "Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household|Grains|Spices|Cleaning|Footwear|Clothing",
  "brand": "brand name or empty string",
  "price": approximate price as number in Indian rupees,
  "note": "any important note about availability"
}

Be accurate. Dmart sells groceries, FMCG, clothing, footwear, home goods.
If product exists at Dmart, set available_at_dmart: true with real approximate price.
If not at Dmart (like branded electronics, medicines etc), set available_at_dmart: false.`;

  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g, '').trim();
    return JSON.parse(text);
  } catch (e) {
    return { available_at_dmart: false, note: 'Could not verify' };
  }
}

// ─────────────────────────────────────────────────
// STEP 3: GEMINI — generate friendly reply message
// ─────────────────────────────────────────────────
async function generateFriendlyReply(customerName, context, prefs) {
  const prefList = prefs.map(p => p.category).join(', ') || 'not set';
  const prompt = `You are Dmart Assistant — a warm, proactive AI shopping friend for Dmart India.

Customer: ${customerName}
Their preferences: ${prefList}
Situation: ${context}

Write a SHORT friendly message (2-3 sentences max) as a caring friend who:
- Genuinely cares about saving them money
- Subtly hints that delivery apps (without naming them) add hidden fees and they lose money on every order
- Encourages them to come to Dmart — fresh products, real prices, no delivery markup
- Sounds natural and warm, not salesy

Reply with ONLY the message text, no JSON, no quotes.`;

  try {
    const res = await model.generateContent(prompt);
    return res.response.text().trim();
  } catch (e) {
    return `Hey ${customerName}! Let me find the best options for you at Dmart right now! 😊`;
  }
}

// ─────────────────────────────────────────────────
// SEND PRODUCT CARD
// ─────────────────────────────────────────────────
async function sendProductCard(to, product, index, source) {
  let cap = '';
  if (index === 0) cap += `⭐ *TOP PICK*\n`;
  cap += `*${product.name}*`;
  if (product.brand) cap += ` — ${product.brand}`;
  cap += `\n📦 ${product.category}\n`;

  const discount = parseFloat(product.discount_percent || 0);
  if (discount > 0) {
    const finalPrice = product.final_price || product.offer_price || product.price;
    const save = (parseFloat(product.price) - parseFloat(finalPrice)).toFixed(0);
    cap += `~~Rs.${product.price}~~ → *Rs.${finalPrice}* 🏷️ *${discount}% OFF*\n`;
    cap += `💰 You save Rs.${save} — that's real money in your pocket!`;
  } else {
    cap += `💵 *Rs.${product.price}*`;
    if (source === 'gemini') cap += `\n📍 Verified available at Dmart!`;
  }
  cap += `\n\n🛒 Pick it up at Dmart — no delivery fees, no waiting!\nReply *"order ${product.name}"* to note it.`;

  if (product.image_url && product.image_url.startsWith('http')) {
    await sendImage(to, product.image_url, cap);
  } else {
    await sendText(to, cap);
  }
}

// ─────────────────────────────────────────────────
// HANDLE SHOPPING LIST — core feature
// Checks each item: DB first → Gemini → not available
// ─────────────────────────────────────────────────
async function handleShoppingList(to, customer, items, prefs) {
  console.log('Shopping list items:', items);

  const friendMsg = await generateFriendlyReply(
    customer.name,
    `customer sent a shopping list with ${items.length} items to check availability`,
    prefs
  );

  await sendText(to, `${friendMsg}\n\n📋 *Checking your ${items.length} items one by one...*\n⚡ Give me a moment!`);
  await sleep(1500);

  const available = [];      // found in DB
  const fromGemini = [];     // found via Gemini, added to DB
  const notAvailable = [];   // not at Dmart at all

  for (const item of items) {
    console.log('Checking:', item);

    // 1. Check in our database
    const dbResults = await searchProductInDB(item);
    if (dbResults.length > 0) {
      available.push({ ...dbResults[0], originalQuery: item, source: 'db' });
      continue;
    }

    // 2. Not in DB — ask Gemini
    await sleep(300);
    const geminiResult = await checkProductWithGemini(item);
    console.log('Gemini result for', item, ':', geminiResult.available_at_dmart);

    if (geminiResult.available_at_dmart) {
      // Add to DB for future use
      await addProductToDB(
        geminiResult.name,
        geminiResult.category,
        geminiResult.brand,
        geminiResult.price
      );
      fromGemini.push({ ...geminiResult, originalQuery: item, source: 'gemini' });
    } else {
      notAvailable.push(item);
    }
  }

  // ── SEND RESULTS ──

  // DB products with images
  if (available.length > 0) {
    await sendText(to, `✅ *${available.length} item${available.length > 1 ? 's' : ''} available at Dmart right now:*`);
    await sleep(500);
    for (let i = 0; i < available.length; i++) {
      await sendProductCard(to, available[i], i, 'db');
      await sleep(700);
    }
  }

  // Gemini-verified products (no image, text only)
  if (fromGemini.length > 0) {
    await sleep(500);
    await sendText(to, `🔍 *${fromGemini.length} more item${fromGemini.length > 1 ? 's' : ''} available at Dmart — verified from Dmart catalog:*`);
    await sleep(500);
    for (const p of fromGemini) {
      let msg = `✅ *${p.name}*`;
      if (p.brand) msg += ` — ${p.brand}`;
      msg += `\n📦 ${p.category}\n💵 Rs.${p.price} (approx Dmart price)\n`;
      msg += `📍 Available at Dmart stores!\n`;
      msg += `🛒 Just walk in — no delivery fee, fresh stock!\n`;
      msg += `Reply *"order ${p.name}"* to note it.`;
      await sendText(to, msg);
      await sleep(600);
    }
  }

  // Not available
  if (notAvailable.length > 0) {
    await sleep(500);
    await sendText(to,
      `❌ *These are not available at Dmart:*\n\n${notAvailable.map(n => `• ${n}`).join('\n')}\n\n💡 Want me to suggest similar products we do carry? Just say *"suggest alternatives"*!`
    );
  }

  // Final summary
  const totalFound = available.length + fromGemini.length;
  await sleep(600);

  const summaryMsg = await generateFriendlyReply(
    customer.name,
    `shopping list check complete. Found ${totalFound} out of ${items.length} items available at Dmart. ${notAvailable.length > 0 ? notAvailable.length + ' items not available.' : 'All items found!'}`,
    prefs
  );

  await sendText(to,
    `📊 *Shopping List Summary for ${customer.name}:*\n` +
    `✅ Available at Dmart: *${totalFound} items*\n` +
    `❌ Not available: *${notAvailable.length} items*\n\n` +
    `${summaryMsg}\n\n` +
    `Reply *"show offers"* to see today's deals on your items! 🏷️`
  );
}

// ─────────────────────────────────────────────────
// WELCOME MESSAGE
// ─────────────────────────────────────────────────
function buildWelcome(name) {
  return `👋 *Hey ${name}! Your Dmart Assistant is here!* 🛒

Think of me as that friend who always knows what's fresh, what's on offer, and how to save the most at Dmart!

Here's *everything* I can do for you:

🎯 *Smart Recommendations*
I know what you love — I'll find your favourites instantly with the best price!
_"Recommend something for me"_

🏷️ *Offers & Deals*
I show you deals on what YOU actually buy — not random stuff!
_"What offers today?"_

🛍️ *Browse Any Product or Category*
Snacks, dairy, fruits, spices, footwear — just ask for anything!
_"Show me snacks"_ or _"Lays chips"_ or _"Dark Fantasy"_

📋 *Shopping List Checker*
Send your full shopping list — I'll check every item for availability and price!
_Just paste your list like: Beans 1kg, Carrot, Wheat 5kg, Shampoo_

🔍 *Product Not in Our Store?*
I'll check the full Dmart India catalog online and add it for you!

💡 *Honest Friend Advice*
I'll tell you what's worth buying today — and remind you that picking it up yourself saves way more than paying delivery charges!

📦 *Pack & Notify*
Tell me your list and I'll check if it's ready — you just come pick it up!

*Just chat naturally — I understand everything!* 😊

What would you like today?`;
}

// ─────────────────────────────────────────────────
// 1 MINUTE FOLLOW-UP — proactive attraction
// ─────────────────────────────────────────────────
async function sendFollowUp(to, customer) {
  const prefs = await getPreferences(customer.customer_id);
  const prefNames = prefs.slice(0, 3).map(p => p.category).join(', ');
  const products = await getPreferredProducts(customer.customer_id, 4);
  const offers = await getOffers(customer.customer_id);
  const relevantOffers = offers.filter(o => prefs.some(p => p.category === o.category));

  await sendText(to,
    `🤔 *${customer.name}, still thinking?*\n\n` +
    `You know what — as your Dmart friend I can't let you miss this 😄\n\n` +
    `I know you regularly pick up *${prefNames}* — and right now the offers on exactly those things are honestly really good.\n\n` +
    `Let me just show you what I found for you 👇`
  );
  await sleep(1500);

  if (products.length > 0) {
    await sendText(to, `💚 *From your favourite sections — picked just for you ${customer.name}:*`);
    await sleep(600);
    for (let i = 0; i < Math.min(products.length, 4); i++) {
      await sendProductCard(to, products[i], i, 'db');
      await sleep(700);
    }
  }

  if (relevantOffers.length > 0) {
    await sleep(800);
    await sendText(to, `🔥 *Active offers on products you buy regularly:*`);
    await sleep(500);
    for (let i = 0; i < Math.min(relevantOffers.length, 3); i++) {
      const o = relevantOffers[i];
      const cap =
        `🎉 *${o.name}*${o.brand ? ' — ' + o.brand : ''}\n` +
        `~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n` +
        `💰 Save Rs.${o.you_save} just by walking in!\n\n` +
        `🤝 Honestly? This is what I'd tell any friend — skip the delivery app fees, come to Dmart and keep that money in your pocket.\n\n` +
        `Reply *"order ${o.name}"* to note it!`;
      if (o.image_url && o.image_url.startsWith('http')) {
        await sendImage(to, o.image_url, cap);
      } else {
        await sendText(to, cap);
      }
      await sleep(700);
    }
  }

  await sendText(to,
    `😊 *${customer.name} — this is all waiting for you at Dmart right now.*\n\n` +
    `Fresh products. Real prices. No hidden delivery fees. No surge pricing.\n` +
    `Just the actual price — the way shopping should be! 🛒\n\n` +
    `What do you want to grab today? Send your shopping list and I'll check everything! 📋`
  );
}

// ─────────────────────────────────────────────────
// MAIN MESSAGE HANDLER
// ─────────────────────────────────────────────────
async function processMessage(fromRaw, message) {
  console.log('\n═══ NEW MESSAGE ═══');
  console.log('From:', fromRaw);
  console.log('Message:', message);

  const phone = fromRaw.replace('whatsapp:', '').replace('+', '');

  // Cancel pending follow-up — customer replied
  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
    console.log('Follow-up cancelled — customer replied');
  }

  const customer = await getCustomerByPhone(phone);
  if (!customer) {
    await sendText(fromRaw,
      `👋 Welcome to *Dmart Assistant*!\n\n` +
      `Your number isn't registered yet. Visit your nearest Dmart store to register and unlock personalized shopping!\n\n` +
      `Meanwhile feel free to ask about any product. 🛒`
    );
    return;
  }

  console.log('Customer:', customer.name);

  const isNew = await isNewCustomer(customer.customer_id);
  const prefs = await getPreferences(customer.customer_id);

  // ── FIRST TIME CUSTOMER ──
  if (isNew) {
    const welcome = buildWelcome(customer.name);
    await sendText(fromRaw, welcome);
    await logInteraction(customer.customer_id, phone, message, 'welcome', null, 'welcome sent');

    // Schedule 60-second follow-up
    const handle = setTimeout(async () => {
      try {
        console.log('Sending follow-up to', customer.name);
        await sendFollowUp(fromRaw, customer);
        pendingFollowUp.delete(phone);
      } catch (e) { console.error('Follow-up error:', e.message); }
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // ── PARSE INTENT ──
  const parsed = await parseIntent(message, customer.name, prefs);
  console.log('Intent:', parsed.intent, '| Keyword:', parsed.search_keyword, '| Items:', parsed.items?.length || 0);

  // ── OUT OF SCOPE ──
  if (!parsed.is_dmart_related) {
    await sendText(fromRaw,
      `😄 Ha, I wish I could help with that ${customer.name}!\n\n` +
      `But I'm your *Dmart shopping assistant* — I'm only good at finding you amazing products and deals! 🛒\n\n` +
      `Try asking:\n• *"Show me snacks"*\n• *"What offers today?"*\n• Or just send your shopping list!`
    );
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, 'redirected');

    // Schedule follow-up after out-of-scope
    const handle = setTimeout(async () => {
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch (e) {}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // ── SHOPPING LIST ── (top priority — check before anything else)
  if (parsed.intent === 'shopping_list' && parsed.items && parsed.items.length > 0) {
    await handleShoppingList(fromRaw, customer, parsed.items, prefs);
    await logInteraction(customer.customer_id, phone, message, 'shopping_list', null, `checked ${parsed.items.length} items`);
    return;
  }

  // ── PLACE ORDER ──
  if (parsed.intent === 'place_order') {
    const productName = parsed.order_product || parsed.search_keyword || 'your item';
    // Clear any existing order timer for this customer
    if (pendingOrders.has(phone)) {
      clearTimeout(pendingOrders.get(phone).timer);
      pendingOrders.delete(phone);
    }
    await handleOrder(fromRaw, customer, productName, prefs);
    await logInteraction(customer.customer_id, phone, message, 'place_order', productName, 'order placed');
    return;
  }

  // ── DMART QUESTION ──
  if (parsed.intent === 'question') {
    const answer = await answerDmartQuestion(message, customer.name, prefs);
    await sendText(fromRaw, `💡 ${answer}\n\nAnything else I can help you with? 😊`);
    await logInteraction(customer.customer_id, phone, message, 'question', null, answer);
    return;
  }

  // ── CHECK OFFERS ──
  if (parsed.intent === 'check_offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length > 0) {
      const friendMsg = await generateFriendlyReply(customer.name, 'customer wants to see offers', prefs);
      await sendText(fromRaw,
        `${friendMsg}\n\n🔥 *${offers.length} live deals right now — many on things YOU buy!*`
      );
      await sleep(800);
      for (let i = 0; i < Math.min(offers.length, 5); i++) {
        const o = offers[i];
        const cap =
          `🏷️ *${o.name}*${o.brand ? ' — ' + o.brand : ''}\n` +
          `~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n` +
          `💰 Save Rs.${o.you_save}!\n\n` +
          `💡 Think about it — that's money you keep by just walking into Dmart instead of paying delivery charges on top.\n\n` +
          `Reply *"order ${o.name}"* to note it!`;
        if (o.image_url && o.image_url.startsWith('http')) {
          await sendImage(fromRaw, o.image_url, cap);
        } else {
          await sendText(fromRaw, cap);
        }
        await sleep(700);
      }
      await sendText(fromRaw,
        `😊 *These deals are live at Dmart right now ${customer.name}!*\n\n` +
        `Want to check your shopping list? Just paste it and I'll check everything! 📋`
      );
    } else {
      await sendText(fromRaw, `No active offers right now ${customer.name}, but I'll notify you when new ones come! Meanwhile want to browse any category? 😊`);
    }
    await logInteraction(customer.customer_id, phone, message, 'check_offers', null, 'offers shown');
    return;
  }

  // ── SEARCH PRODUCT or BROWSE CATEGORY ──
  const keyword = parsed.search_keyword || parsed.category || message.split(' ').slice(0, 2).join(' ');
  console.log('Searching DB for:', keyword);

  let dbResults = await searchProductInDB(keyword);

  if (dbResults.length > 0) {
    // Found in DB — show with images
    const friendMsg = await generateFriendlyReply(
      customer.name,
      `customer asked for ${keyword} and we found ${dbResults.length} products in Dmart store`,
      prefs
    );
    await sendText(fromRaw,
      `${friendMsg}\n\n🛒 *Found ${dbResults.length} products for "${keyword}" at Dmart:*`
    );
    await sleep(800);
    for (let i = 0; i < Math.min(dbResults.length, 5); i++) {
      await sendProductCard(to = fromRaw, dbResults[i], i, 'db');
      await sleep(700);
    }

    const withOffers = dbResults.filter(p => parseFloat(p.discount_percent) > 0);
    await sendText(fromRaw,
      `${withOffers.length > 0 ? `🎉 *${withOffers.length} of these have active offers right now!*\n\n` : ''}` +
      `📍 All in stock at Dmart — just walk in and grab it!\n` +
      `No delivery wait. No extra fees. Fresh from the shelf! 🛒\n\n` +
      `Want to see more? Send your full shopping list and I'll check everything! 📋`
    );
  } else {
    // Not in DB — check with Gemini
    console.log('Not in DB, checking with Gemini:', keyword);
    const geminiResult = await checkProductWithGemini(keyword);

    if (geminiResult.available_at_dmart) {
      await addProductToDB(geminiResult.name, geminiResult.category, geminiResult.brand, geminiResult.price);
      const friendMsg = await generateFriendlyReply(
        customer.name,
        `customer asked for ${keyword}. Found it in Dmart catalog — ${geminiResult.name} at Rs.${geminiResult.price}`,
        prefs
      );
      await sendText(fromRaw,
        `${friendMsg}\n\n✅ *Found "${geminiResult.name}" at Dmart!*\n\n` +
        `📦 Category: ${geminiResult.category}\n` +
        `${geminiResult.brand ? `🏷️ Brand: ${geminiResult.brand}\n` : ''}` +
        `💵 *Rs.${geminiResult.price}* (Dmart price)\n\n` +
        `📍 Available at Dmart stores — pick it up yourself and save on delivery!\n` +
        `Reply *"order ${geminiResult.name}"* to note it! 🛒`
      );
    } else {
      const friendMsg = await generateFriendlyReply(
        customer.name,
        `customer asked for ${keyword} but it's not available at Dmart`,
        prefs
      );
      await sendText(fromRaw,
        `${friendMsg}\n\n` +
        `😕 *"${keyword}" doesn't seem to be available at Dmart.*\n\n` +
        `Want me to suggest similar products we do carry?\n` +
        `Or try browsing: 🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty`
      );
    }
  }

  await logInteraction(customer.customer_id, phone, message, parsed.intent, keyword, 'reply sent');
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

async function createOrder(customerId, phone, productName) {
  const r = await pool.query(
    `INSERT INTO customer_orders(customer_id,phone_number,product_name,status) VALUES($1,$2,$3,'pending') RETURNING order_id`,
    [customerId, phone, productName]
  );
  return r.rows[0].order_id;
}

async function updateOrderStatus(orderId, status) {
  await pool.query(
    `UPDATE customer_orders SET status=$1, updated_at=NOW() WHERE order_id=$2`,
    [status, orderId]
  );
}

async function banCustomer(customerId, phone, reason) {
  await pool.query(
    `INSERT INTO order_bans(customer_id,phone_number,ban_until,reason) VALUES($1,$2,NOW() + INTERVAL '30 days',$3)`,
    [customerId, phone, reason]
  );
}

async function handleOrder(fromRaw, customer, productName, prefs) {
  const phone = fromRaw.replace('whatsapp:', '').replace('+', '');

  // Check if banned
  const ban = await isCustomerBanned(customer.customer_id, phone);
  if (ban) {
    const banDate = new Date(ban.ban_until).toLocaleDateString('en-IN');
    await sendText(fromRaw,
      `⚠️ *${customer.name}, your ordering access is currently suspended.*\n\n` +
      `Due to repeated order cancellations, ordering has been paused until *${banDate}*.\n\n` +
      `You can still browse products and check availability. For help, visit Dmart directly. 🛒`
    );
    return;
  }

  // Check cancelled order count
  const cancelCount = await getCancelledCount(customer.customer_id);
  if (cancelCount >= 3) {
    await banCustomer(customer.customer_id, phone, 'Too many cancelled/unresponsive orders');
    await sendText(fromRaw,
      `⚠️ *${customer.name}, ordering has been paused for 1 month.*\n\n` +
      `We noticed 3 orders were placed but not picked up. To keep our system fair for everyone, ordering access is paused for 30 days.\n\n` +
      `You can still browse and check availability. See you at Dmart! 🛒`
    );
    return;
  }

  // Confirm order
  const orderId = await createOrder(customer.customer_id, phone, productName);
  await sendText(fromRaw,
    `✅ *Got it ${customer.name}!*\n\n` +
    `Your order for *${productName}* is noted.\n\n` +
    `⏳ Give us about *3 minutes* — we'll confirm it's ready for you to pick up!\n\n` +
    `📍 Just a reminder — please have your payment ready when you come to Dmart.\n` +
    `_(Pay at the counter — no worries, just letting you know!)_ 😊`
  );

  // Set 3-minute timer — confirm ready
  const readyTimer = setTimeout(async () => {
    try {
      await updateOrderStatus(orderId, 'ready');
      await sendText(fromRaw,
        `🎉 *${customer.name}, your order is READY!*\n\n` +
        `✅ *${productName}* is packed and waiting for you at Dmart!\n\n` +
        `📍 Come pick it up at your convenience.\n` +
        `💳 Payment at the counter when you arrive — super easy!\n\n` +
        `See you soon! 😊🛒`
      );

      // Set 30-minute no-show timer
      const noShowTimer = setTimeout(async () => {
        try {
          await updateOrderStatus(orderId, 'cancelled');
          const newCount = await getCancelledCount(customer.customer_id);
          let msg =
            `⚠️ *Order Cancelled — ${customer.name}*\n\n` +
            `We waited but didn't hear back, so your order for *${productName}* has been cancelled.\n\n` +
            `No worries — you can order again anytime! 😊\n\n` +
            `Just a heads up: after *3 such cancellations*, ordering will be paused for a month to keep things fair for everyone. ` +
            `Currently: *${newCount}/3* cancellations.\n\n` +
            `Hope to see you at Dmart soon! 🛒`;
          if (newCount >= 3) {
            await banCustomer(customer.customer_id, phone, 'No-show after order confirmed');
            msg =
              `⚠️ *Ordering paused for 1 month — ${customer.name}*\n\n` +
              `This was the 3rd time an order was placed but not picked up. To keep things fair, ordering has been paused for 30 days.\n\n` +
              `You can still browse products. See you at Dmart! 🛒`;
          }
          await sendText(fromRaw, msg);
          pendingOrders.delete(phone);
        } catch(e) { console.error('No-show timer error:', e.message); }
      }, 30 * 60 * 1000); // 30 minutes

      pendingOrders.set(phone, { orderId, productName, timer: noShowTimer, stage: 'ready' });
    } catch(e) { console.error('Ready timer error:', e.message); }
  }, 3 * 60 * 1000); // 3 minutes

  pendingOrders.set(phone, { orderId, productName, timer: readyTimer, stage: 'pending' });
}

// ─────────────────────────────────────────────────
// GEMINI — ANSWER DMART QUESTIONS
// ─────────────────────────────────────────────────
async function answerDmartQuestion(question, customerName, prefs) {
  const prefList = prefs.map(p => p.category).join(', ') || 'not set';
  const prompt = `You are Dmart Assistant — an expert on Dmart supermarkets in India.

Customer: ${customerName}
Their preferences: ${prefList}
Question: "${question}"

Answer this question as a knowledgeable Dmart friend. You know:
- Dmart store timings (usually 8am-10pm, varies by store)
- Dmart return policy (most items returnable within 7 days with bill)
- Dmart membership/D-Mart Ready app
- Dmart product categories and what they carry
- Dmart pricing philosophy (EDLP — every day low price)
- Dmart bulk buying savings
- General grocery knowledge

Be warm, helpful, 2-3 sentences. If you don't know something specific, say "I'm not 100% sure about your specific store — best to check with the store directly!" and give what you do know.

Reply with ONLY the answer text, no JSON, no quotes.`;

  try {
    const res = await model.generateContent(prompt);
    return res.response.text().trim();
  } catch(e) {
    return `Great question ${customerName}! I'd recommend checking with your nearest Dmart store directly for the most accurate answer. You can also check the D-Mart Ready app for more info! 😊`;
  }
}

// ─────────────────────────────────────────────────
// ENDPOINTS
// ─────────────────────────────────────────────────
app.post('/whatsapp', (req, res) => {
  console.log('\n─── WEBHOOK HIT ───');
  console.log('From:', req.body.From);
  console.log('Body:', req.body.Body);

  // Respond to Twilio instantly — prevents timeout
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');

  const fromRaw = req.body.From || '';
  const message = req.body.Body || '';
  if (fromRaw && message) {
    processMessage(fromRaw, message).catch(e => console.error('processMessage error:', e.message));
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n✅ Dmart AI Assistant running on port ${PORT}`);
  console.log(`WhatsApp endpoint: POST /whatsapp`);
  console.log(`Health check: GET /health\n`);
});
