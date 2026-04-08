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
        console.log('Key ' + (currentKeyIndex + 1) + ' exhausted — rotating');
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
const pendingFollowUp = new Map();
const pendingOrders = new Map();
const customerContext = new Map(); // phone -> { availableItems, notAvailableItems, lastProducts, lastIntent }

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────────────
// SETUP DB TABLES
// ─────────────────────────────────────────────────
async function ensureTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_interactions (
        id SERIAL PRIMARY KEY, customer_id INTEGER, phone_number VARCHAR(20),
        message TEXT, intent VARCHAR(50), category VARCHAR(50),
        bot_response TEXT, created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS customer_orders (
        order_id SERIAL PRIMARY KEY, customer_id INTEGER, phone_number VARCHAR(20),
        product_name TEXT, status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS order_bans (
        ban_id SERIAL PRIMARY KEY, customer_id INTEGER, phone_number VARCHAR(20),
        ban_until TIMESTAMP, reason TEXT, created_at TIMESTAMP DEFAULT NOW()
      );
    `);
  } catch(e) { console.log('Table setup note:', e.message); }
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
  } catch (e) { await sendText(to, caption); }
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

// Fetch FULL product catalog to give Gemini as context — this is the key change
async function getAllProductsContext() {
  const r = await pool.query(`
    SELECT p.name, p.category, p.brand, p.price, p.is_available,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
    ORDER BY p.category, p.name`);
  return r.rows;
}

// Fetch products by list of names Gemini identified
async function fetchProductsByNames(names) {
  if (!names || names.length === 0) return [];
  const conditions = names.map((_, i) =>
    `(LOWER(p.name) LIKE LOWER($${i+1}) OR LOWER(p.brand) LIKE LOWER($${i+1}) OR LOWER(p.category) LIKE LOWER($${i+1}))`
  ).join(' OR ');
  const params = names.map(n => `%${n}%`);
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0 AND (${conditions})
    ORDER BY COALESCE(o.discount_percent,0) DESC`, params);
  return r.rows;
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
    console.log('Saved to DB:', name);
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
// THE BRAIN — Gemini reads full message + full catalog
// Understands like a human, matches products smartly
// ─────────────────────────────────────────────────
async function thinkAndDecide(message, customer, prefs, allProducts, context) {
  const prefList = prefs.map(p => p.category).join(', ') || 'General';

  // Give Gemini the full catalog so it can actually match
  const catalog = allProducts.map(p =>
    `${p.name}|${p.category}|${p.brand || ''}|Rs.${p.price}${parseFloat(p.discount_percent) > 0 ? `|${p.discount_percent}%OFF→Rs.${p.final_price}` : ''}`
  ).join('\n');

  const ctxSection = context ? `
PREVIOUS CONTEXT (what happened before this message):
- Last action: ${context.lastIntent}
- Available items from last check: ${(context.availableItems || []).join(', ') || 'none'}
- Not available last time: ${(context.notAvailableItems || []).join(', ') || 'none'}
- Last products shown: ${(context.lastProducts || []).slice(0, 4).map(p => p.name).join(', ') || 'none'}
NOTE: If customer's new message is a response to the above (like "okay pack available", "order those", "except unavailable pack rest") — set intent to "follow_up".` : '';

  const prompt = `You are Dmart India's WhatsApp shopping assistant. Think like a human friend who works at Dmart and knows the customer well.

CUSTOMER: ${customer.name}
CUSTOMER LIKES: ${prefList}
CUSTOMER SAYS: "${message}"
${ctxSection}

OUR FULL PRODUCT CATALOG (name|category|brand|price|offer):
${catalog || 'No products loaded yet'}

YOUR TASK:
Read the customer message fully. Understand EXACTLY what they are asking or saying. Match their request intelligently to our product catalog.

Reply ONLY with valid JSON — no markdown, no explanation outside the JSON:
{
  "intent": "greeting|shopping_list|search|browse_category|offers|order|follow_up|question|out_of_scope",
  "understood_as": "what you understood the customer is asking — 1 sentence",
  "matched_products": ["exact product names from our catalog that match what they want"],
  "search_terms": ["additional keywords to search DB with"],
  "unknown_items": ["items customer asked for that are NOT anywhere in our catalog"],
  "order_items": ["product names if they are ordering"],
  "shopping_list_items": ["each individual item if it is a shopping list — WITHOUT quantities"],
  "reply": "warm, friendly 2-3 sentence reply as a caring shopping friend. For product requests: subtly mention picking up from Dmart = real prices, no delivery markup. Sound natural, not robotic.",
  "dmart_answer": null
}

INTENT CLASSIFICATION — read every rule:
- greeting: hi/hello/hey/good morning/good evening — ONLY a greeting with no product request
- shopping_list: customer lists 2+ different items they want to check — each item goes in shopping_list_items[] WITHOUT quantities e.g. "Beans 1kg, Carrot 1kg, Lays" → items are ["Beans","Carrot","Lays"]
- search: asking about 1-2 specific products by name
- browse_category: "show me snacks" / "what dairy products do you have" / "show all fruits"
- offers: "what offers" / "any deals" / "discounts"
- order: "order it" / "pack it" / "I'll pick it up" / "book it"
- follow_up: customer replies to previous list result like "okay pack available ones", "order those", "except unavailable pack rest"
- question: asking about store hours, return policy, parking, D-Mart Ready app, bot capabilities ("what can you do") — put the answer in dmart_answer
- out_of_scope: cricket, movies, politics, weather, jokes — completely unrelated to shopping

CRITICAL MATCHING RULES:
- "Okay check and tell me the snacks items" → intent: browse_category, search_terms: ["Snacks"]
- "What are the things you can do?" → intent: question, answer in dmart_answer about bot capabilities
- "Hi show me snacks" → intent: browse_category (the Hi is just greeting before the request)
- When searching: match "Lays" to "Lays Classic" etc. Be smart about partial and fuzzy matching
- Shopping list items — strip quantities: "Beans 1kg" → "Beans", "Lays 20rs 5 packets" → "Lays"
- If a product is in our catalog, it goes in matched_products. If NOT in catalog, put in unknown_items
- Categories available: Snacks, Dairy, Fruits, Vegetables, Instant Food, Beverages, Beauty, Personal Care, Household, Grains, Spices, Cleaning, Footwear`;

  try {
    const raw = await callGemini(prompt);
    if (!raw) throw new Error('No Gemini response');
    const clean = raw.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);
    parsed.matched_products = parsed.matched_products || [];
    parsed.search_terms = parsed.search_terms || [];
    parsed.unknown_items = parsed.unknown_items || [];
    parsed.order_items = parsed.order_items || [];
    parsed.shopping_list_items = parsed.shopping_list_items || [];
    parsed.reply = parsed.reply || `On it ${customer.name}! 😊`;
    console.log('Brain understood:', parsed.understood_as);
    console.log('Intent:', parsed.intent, '| Matched:', parsed.matched_products.length, '| Unknown:', parsed.unknown_items.length, '| List items:', parsed.shopping_list_items.length);
    return parsed;
  } catch(e) {
    console.log('thinkAndDecide error:', e.message);
    return {
      intent: 'search',
      understood_as: 'Customer sent a message',
      matched_products: [],
      search_terms: [message.split(' ').filter(w => w.length > 2)[0] || message],
      unknown_items: [],
      order_items: [],
      reply: `On it ${customer.name}! Let me check that for you 😊`,
      dmart_answer: null,
      shopping_list_items: []
    };
  }
}

// Check unknown items with Gemini — ALL at once (1 call for multiple unknowns)
async function checkUnknownWithGemini(items) {
  if (!items || items.length === 0) return [];
  const prompt = `You are a Dmart India product expert. Check if these items are sold at Dmart India stores.
Items to check: ${items.join(', ')}
Reply ONLY valid JSON array, no markdown:
[{"item":"original item name","available":true,"dmart_name":"exact name as sold at Dmart","category":"category","brand":"brand or empty string","price":approximate_price_number}]
Note: Dmart sells groceries, FMCG, clothing, footwear, home goods, stationery. NOT medicines, NOT electronics.
If not at Dmart, still include it: {"item":"name","available":false,"dmart_name":"","category":"","brand":"","price":0}`;
  try {
    const raw = await callGemini(prompt);
    if (!raw) return [];
    return JSON.parse(raw.replace(/```json|```/g, '').trim());
  } catch(e) { return []; }
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
// WELCOME
// ─────────────────────────────────────────────────
function buildWelcome(name) {
  return `👋 *Hey ${name}! Welcome to Dmart Assistant!* 🛒

I'm your personal shopping friend at Dmart. Here's everything I can do:

🔍 *Find any product* — "Do you have Lays?" or "Show me dairy products"
📋 *Check your shopping list* — Send your full list, I'll check what's available with prices and offers
🏷️ *Show live deals* — "What offers are there today?"
📦 *Note your pickup order* — "Pack it, I'll pick it up"
💡 *Answer any Dmart question* — store timings, return policy, Dmart Ready app, parking — anything!
🤝 *Personal picks* — I know what you like and show you the best deals on those

I'm here to make your Dmart shopping smarter. 😊

*What do you need today?*`;
}

// ─────────────────────────────────────────────────
// FOLLOW UP — 0 Gemini calls
// ─────────────────────────────────────────────────
async function sendFollowUp(to, customer) {
  const prods = await getPreferredProducts(customer.customer_id, 3);
  if (prods.length === 0) {
    await sendText(to, `👋 *${customer.name}!* Anything you need from Dmart today? Just ask or send your list! 😊`);
    return;
  }
  const highlight = prods.find(p => parseFloat(p.discount_percent) > 0) || prods[0];
  await sendText(to,
    `🛒 *${customer.name}, I know what you love at Dmart!*\n\n` +
    `*${highlight.name}* is ${parseFloat(highlight.discount_percent) > 0
      ? `at *${highlight.discount_percent}% OFF* — only Rs.${highlight.final_price} (was Rs.${highlight.price})!`
      : `in stock at Rs.${highlight.price}.`}\n\n` +
    `Fresh products, real prices — no delivery wait, no hidden fees. Just walk in! 😊\n\n` +
    `Send your list or ask for anything! 📋`
  );
  await sleep(500);
  for (let i = 0; i < Math.min(prods.length, 2); i++) {
    await sendProductCard(to, prods[i], i === 0);
    await sleep(500);
  }
}

// ─────────────────────────────────────────────────
// ORDER SYSTEM
// ─────────────────────────────────────────────────
async function isCustomerBanned(customerId, phone) {
  try {
    const r = await pool.query(`SELECT * FROM order_bans WHERE (customer_id=$1 OR phone_number=$2) AND ban_until > NOW() LIMIT 1`, [customerId, phone]);
    return r.rows[0] || null;
  } catch(e) { return null; }
}

async function getCancelledCount(customerId) {
  try {
    const r = await pool.query(`SELECT COUNT(*) as c FROM customer_orders WHERE customer_id=$1 AND status='cancelled' AND created_at > NOW() - INTERVAL '30 days'`, [customerId]);
    return parseInt(r.rows[0].c);
  } catch(e) { return 0; }
}

async function handleOrder(to, customer, productNames) {
  const phone = to.replace('whatsapp:', '').replace('+', '');
  const ban = await isCustomerBanned(customer.customer_id, phone);
  if (ban) {
    await sendText(to, `⚠️ *${customer.name}, ordering is paused until ${new Date(ban.ban_until).toLocaleDateString('en-IN')}.*\n\nYou can still browse products! 🛒`);
    return;
  }
  const cancelCount = await getCancelledCount(customer.customer_id);
  if (cancelCount >= 3) {
    try { await pool.query(`INSERT INTO order_bans(customer_id,phone_number,ban_until,reason) VALUES($1,$2,NOW() + INTERVAL '30 days',$3)`, [customer.customer_id, phone, 'Too many cancelled orders']); } catch(e) {}
    await sendText(to, `⚠️ *${customer.name}, ordering paused for 30 days.*\n\n3 orders weren't picked up. Paused to keep things fair. You can still browse! 😊`);
    return;
  }

  const itemList = Array.isArray(productNames) ? productNames : [productNames];
  const orderIds = [];
  for (const item of itemList.slice(0, 6)) {
    try {
      const r = await pool.query(`INSERT INTO customer_orders(customer_id,phone_number,product_name,status) VALUES($1,$2,$3,'pending') RETURNING order_id`, [customer.customer_id, phone, item]);
      orderIds.push(r.rows[0].order_id);
    } catch(e) {}
  }

  await sendText(to,
    `✅ *Got it ${customer.name}!*\n\n` +
    `Order noted for:\n${itemList.map(i => `• ${i}`).join('\n')}\n\n` +
    `⏳ Give us *3 minutes* to prepare!\n\n` +
    `📍 Please have payment ready when you come. _(Pay at the counter!)_ 😊`
  );

  if (pendingOrders.has(phone)) { clearTimeout(pendingOrders.get(phone).timer); }

  const readyTimer = setTimeout(async () => {
    try {
      for (const oid of orderIds) { await pool.query(`UPDATE customer_orders SET status='ready', updated_at=NOW() WHERE order_id=$1`, [oid]); }
      await sendText(to,
        `🎉 *${customer.name}, your order is READY!*\n\n✅ Packed and waiting:\n${itemList.map(i => `• ${i}`).join('\n')}\n\n📍 Come pick it up anytime. Payment at the counter! 😊`
      );
      const noShowTimer = setTimeout(async () => {
        try {
          for (const oid of orderIds) { await pool.query(`UPDATE customer_orders SET status='cancelled', updated_at=NOW() WHERE order_id=$1`, [oid]); }
          const count = await getCancelledCount(customer.customer_id);
          await sendText(to, `⚠️ *Order cancelled — ${customer.name}*\n\nYou didn't come, so the order was cancelled. No worries, order again anytime! 😊\n_(${count}/3 cancellations — after 3, ordering pauses for 1 month.)_`);
          pendingOrders.delete(phone);
        } catch(e) {}
      }, 30 * 60 * 1000);
      pendingOrders.set(phone, { timer: noShowTimer, stage: 'ready' });
    } catch(e) {}
  }, 3 * 60 * 1000);

  pendingOrders.set(phone, { timer: readyTimer, stage: 'pending' });
}

// ─────────────────────────────────────────────────
// MAIN PROCESSOR
// ─────────────────────────────────────────────────
async function processMessage(fromRaw, message) {
  console.log('\n═══ MESSAGE ═══ From:', fromRaw, '| Body:', message);
  const phone = fromRaw.replace('whatsapp:', '').replace('+', '');

  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
  }

  const customer = await getCustomerByPhone(phone);
  if (!customer) {
    await sendText(fromRaw, `👋 *Welcome to Dmart Assistant!*\n\nYour number isn't registered yet. Visit Dmart to register!\n\nYou can still ask about any product. 🛒`);
    return;
  }

  console.log('Customer:', customer.name);
  const prefs = await getPreferences(customer.customer_id);
  const isNew = await isNewCustomer(customer.customer_id);

  if (isNew) {
    await sendText(fromRaw, buildWelcome(customer.name));
    await logInteraction(customer.customer_id, phone, message, 'welcome', null, 'welcome sent');
    const handle = setTimeout(async () => {
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch(e) {}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // Load full catalog + context, then let Gemini think
  const allProducts = await getAllProductsContext();
  const context = customerContext.get(phone) || null;
  const brain = await thinkAndDecide(message, customer, prefs, allProducts, context);

  // ── GREETING ──
  if (brain.intent === 'greeting') {
    const offers = await getOffers(customer.customer_id);
    await sendText(fromRaw,
      `${brain.reply}\n\n` +
      (offers.length > 0 ? `🔥 *${offers.length} deals* live right now, some on things you love! Ask *"show offers"* to see them.\n\n` : '') +
      `What do you need today? 😊`
    );
    await logInteraction(customer.customer_id, phone, message, 'greeting', null, 'greeted');
    return;
  }

  // ── QUESTION ──
  if (brain.intent === 'question') {
    const answer = brain.dmart_answer || `I can help you find products, check your shopping list, show deals, answer Dmart questions, and note your pickup orders! Just tell me what you need. 😊`;
    await sendText(fromRaw, `💡 ${answer}\n\nAnything else I can help with? 😊`);
    await logInteraction(customer.customer_id, phone, message, 'question', null, answer);
    return;
  }

  // ── OUT OF SCOPE ──
  if (brain.intent === 'out_of_scope') {
    await sendText(fromRaw,
      `😄 Ha! I wish I could help with that ${customer.name}!\n\nBut I'm your *Dmart shopping friend* — only good at products and deals! 🛒\n\nTry: *"Show me snacks"* or send your shopping list!`
    );
    const handle = setTimeout(async () => {
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch(e) {}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, 'redirected');
    return;
  }

  // ── FOLLOW-UP (from previous list) ──
  if (brain.intent === 'follow_up' && context && context.availableItems && context.availableItems.length > 0) {
    await handleOrder(fromRaw, customer, context.availableItems);
    await logInteraction(customer.customer_id, phone, message, 'follow_up_order', null, 'ordered available items');
    return;
  }

  // ── OFFERS ──
  if (brain.intent === 'offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length === 0) {
      await sendText(fromRaw, `No active offers right now ${customer.name}! Want to browse products? 😊`);
      return;
    }
    await sendText(fromRaw, `${brain.reply}\n\n🔥 *${offers.length} live deals — including on things YOU like!*`);
    await sleep(600);
    for (let i = 0; i < Math.min(offers.length, 5); i++) {
      const o = offers[i];
      const cap = `🏷️ *${o.name}*${o.brand ? ' — ' + o.brand : ''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n💰 Save Rs.${o.you_save}!\n\n📍 In stock at Dmart — grab it yourself and keep that extra money! 😊`;
      if (o.image_url && o.image_url.startsWith('http')) { await sendImage(fromRaw, o.image_url, cap); }
      else { await sendText(fromRaw, cap); }
      await sleep(600);
    }
    await sendText(fromRaw, `Want to check your shopping list? Just paste it! 📋`);
    await logInteraction(customer.customer_id, phone, message, 'offers', null, 'offers shown');
    return;
  }

  // ── ORDER ──
  if (brain.intent === 'order') {
    const items = brain.order_items.length > 0 ? brain.order_items : brain.matched_products;
    const orderTarget = items.length > 0 ? items : ['your items'];
    await handleOrder(fromRaw, customer, orderTarget);
    await logInteraction(customer.customer_id, phone, message, 'order', null, orderTarget.join(', '));
    return;
  }

  // ── SHOPPING LIST ──
  if (brain.intent === 'shopping_list' && brain.shopping_list_items.length >= 2) {
    const items = brain.shopping_list_items;
    await sendText(fromRaw, `${brain.reply}\n\n📋 *Checking your ${items.length} items at Dmart...*`);

    // ONE DB query for all items
    const dbProducts = await fetchProductsByNames(items);
    const availableItems = [];
    const unknownItems = [];
    const foundProducts = [];

    for (const item of items) {
      const kl = item.toLowerCase();
      const match = dbProducts.find(p =>
        p.name.toLowerCase().includes(kl) ||
        (p.brand && p.brand.toLowerCase().includes(kl)) ||
        p.category.toLowerCase().includes(kl) ||
        kl.includes(p.name.toLowerCase().split(' ')[0])
      );
      if (match) {
        availableItems.push(item);
        if (!foundProducts.find(fp => fp.product_id === match.product_id)) foundProducts.push(match);
      } else {
        unknownItems.push(item);
      }
    }

    // ONE Gemini call for ALL unknown items
    const geminiResults = unknownItems.length > 0 ? await checkUnknownWithGemini(unknownItems) : [];
    const stillNotAvailable = [];

    for (const gr of geminiResults) {
      if (gr.available) {
        availableItems.push(gr.item);
        addProductToDB(gr.dmart_name, gr.category, gr.brand || '', gr.price);
        foundProducts.push({ name: gr.dmart_name, category: gr.category, brand: gr.brand, price: gr.price, discount_percent: 0, final_price: gr.price, image_url: '' });
      } else {
        stillNotAvailable.push(gr.item);
      }
    }

    // Items Gemini didn't return info for
    for (const item of unknownItems) {
      const hasResult = geminiResults.find(g => g.item && g.item.toLowerCase() === item.toLowerCase());
      if (!hasResult) stillNotAvailable.push(item);
    }

    await sleep(500);
    let report = `📋 *Shopping List — ${customer.name}:*\n\n`;
    if (availableItems.length > 0) {
      report += `✅ *Available at Dmart (${availableItems.length}/${items.length}):*\n`;
      for (const p of foundProducts) {
        const d = parseFloat(p.discount_percent || 0);
        report += `• *${p.name}* — Rs.${d > 0 ? p.final_price + ` *(${d}% OFF)*` : p.price}\n`;
      }
    }
    if (stillNotAvailable.length > 0) {
      report += `\n❌ *Not available at Dmart (${stillNotAvailable.length}):*\n`;
      report += stillNotAvailable.map(i => `• ${i}`).join('\n');
    }
    await sendText(fromRaw, report);
    await sleep(600);

    // Send product cards (max 3 with images)
    const withImages = foundProducts.filter(p => p.image_url && p.image_url.startsWith('http'));
    for (let i = 0; i < Math.min(withImages.length, 3); i++) {
      await sendProductCard(fromRaw, withImages[i], i === 0);
      await sleep(600);
    }

    const totalSave = foundProducts.reduce((acc, p) => {
      const d = parseFloat(p.discount_percent || 0);
      return acc + (d > 0 ? parseFloat(p.price) - parseFloat(p.final_price) : 0);
    }, 0);

    await sendText(fromRaw,
      `🛒 *${availableItems.length} items ready at Dmart!*` +
      (totalSave > 0 ? ` Save Rs.${totalSave.toFixed(0)} with active offers!` : '') +
      `\n\nJust walk in — everything's fresh. No delivery wait, no extra fees! 😊\n\n` +
      `Say *"pack it, I'll pick it up"* to note your order! 📦`
    );

    customerContext.set(phone, { lastIntent: 'shopping_list', availableItems, notAvailableItems: stillNotAvailable, lastProducts: foundProducts });
    await logInteraction(customer.customer_id, phone, message, 'shopping_list', null, `${availableItems.length}/${items.length} available`);
    return;
  }

  // ── SEARCH or BROWSE CATEGORY ──
  const allSearchTerms = [...new Set([...brain.matched_products, ...brain.search_terms])].filter(Boolean);
  let dbResults = allSearchTerms.length > 0 ? await fetchProductsByNames(allSearchTerms) : [];

  // Deduplicate
  const seen = new Set();
  dbResults = dbResults.filter(p => { if (seen.has(p.product_id)) return false; seen.add(p.product_id); return true; });

  if (dbResults.length > 0) {
    await sendText(fromRaw, `${brain.reply}\n\n🛒 *Found ${dbResults.length} product${dbResults.length > 1 ? 's' : ''} at Dmart:*`);
    await sleep(600);
    for (let i = 0; i < Math.min(dbResults.length, 5); i++) {
      await sendProductCard(fromRaw, dbResults[i], i === 0);
      await sleep(600);
    }
    const offersCount = dbResults.filter(p => parseFloat(p.discount_percent) > 0).length;
    await sendText(fromRaw,
      (offersCount > 0 ? `🎉 *${offersCount} of these have active offers!*\n\n` : '') +
      `📍 All in stock — just walk in. No delivery wait, no extra fees! 🛒\n\nNeed more? Send your full list! 📋`
    );
    customerContext.set(phone, { lastIntent: brain.intent, availableItems: dbResults.map(p => p.name), notAvailableItems: brain.unknown_items, lastProducts: dbResults });

  } else if (brain.unknown_items.length > 0) {
    // Not in DB at all — check with Gemini
    console.log('Unknown items — checking with Gemini:', brain.unknown_items);
    const geminiResults = await checkUnknownWithGemini(brain.unknown_items);
    const foundViaGemini = geminiResults.filter(g => g.available);
    const notFoundItems = geminiResults.filter(g => !g.available).map(g => g.item);

    if (foundViaGemini.length > 0) {
      for (const g of foundViaGemini) { addProductToDB(g.dmart_name, g.category, g.brand || '', g.price); }
      await sendText(fromRaw, `${brain.reply}\n\n✅ *Found at Dmart:*`);
      await sleep(400);
      for (const g of foundViaGemini) {
        await sendText(fromRaw, `*${g.dmart_name}*${g.brand ? ` — ${g.brand}` : ''}\n📦 ${g.category}\n💵 *Rs.${g.price}*\n\n📍 Available at Dmart — pick it up and save on delivery! 😊`);
        await sleep(500);
      }
      if (notFoundItems.length > 0) {
        await sendText(fromRaw, `😕 *Not available at Dmart:*\n${notFoundItems.map(i => `• ${i}`).join('\n')}`);
      }
    } else {
      await sendText(fromRaw,
        `${brain.reply}\n\n😕 *Couldn't find that at Dmart.*\n\nWant to browse a category?\n🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🌾 Grains | 👟 Footwear`
      );
    }
  } else {
    await sendText(fromRaw,
      `${brain.reply}\n\n😕 *Nothing matched in our store for that.*\n\nTry browsing:\n🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🌾 Grains`
    );
  }

  await logInteraction(customer.customer_id, phone, message, brain.intent, brain.search_terms[0] || null, 'replied');
}

// ─────────────────────────────────────────────────
// ENDPOINTS
// ─────────────────────────────────────────────────
app.post('/whatsapp', (req, res) => {
  console.log('\n─── WEBHOOK ─── From:', req.body.From, '| Msg:', req.body.Body);
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');
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
  keys: keyStatus.map(k => ({ key: k.index + 1, calls: k.calls, exhausted: k.exhausted }))
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`\n✅ Dmart Assistant running on port ${PORT}\n`));
