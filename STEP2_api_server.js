// ═══════════════════════════════════════════════════════════════
// DMART ASSISTANT — PRODUCTION SERVER
// Complete, robust, no silent failures, full logging
// ═══════════════════════════════════════════════════════════════
'use strict';
require('dotenv').config();
const express  = require('express');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const cors     = require('cors');
const twilio   = require('twilio');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// ── STARTUP VALIDATION ──────────────────────────────────────────
const REQUIRED_ENV = ['DATABASE_URL','TWILIO_ACCOUNT_SID','TWILIO_AUTH_TOKEN','TWILIO_WHATSAPP_NUMBER'];
for (const k of REQUIRED_ENV) {
  if (!process.env[k]) { console.error(`FATAL: missing ${k}`); process.exit(1); }
}

// ── DATABASE ────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});
pool.on('error', e => console.error('DB pool error:', e.message));

// ── GEMINI KEY ROTATION ─────────────────────────────────────────
const GEMINI_KEYS = [];
for (let i = 1; i <= 20; i++) {
  const k = process.env[`GEMINI_API_KEY_${i}`];
  if (k && k.trim()) GEMINI_KEYS.push(k.trim());
}
if (process.env.GEMINI_API_KEY) GEMINI_KEYS.push(process.env.GEMINI_API_KEY);
if (GEMINI_KEYS.length === 0) { console.error('FATAL: no Gemini keys'); process.exit(1); }
console.log(`✅ Loaded ${GEMINI_KEYS.length} Gemini key(s)`);

let activeKeyIdx = 0;
const keyStats = GEMINI_KEYS.map((_, i) => ({ i, calls:0, errors:0, exhausted:false, resetAt:0 }));

function rotateGeminiKey() {
  const start = activeKeyIdx;
  for (let n = 1; n <= GEMINI_KEYS.length; n++) {
    const next = (start + n) % GEMINI_KEYS.length;
    if (!keyStats[next].exhausted) {
      activeKeyIdx = next;
      console.log(`🔄 Gemini rotated → key ${activeKeyIdx + 1}/${GEMINI_KEYS.length}`);
      return;
    }
  }
  // all exhausted — reset all and start from beginning
  keyStats.forEach(k => { k.exhausted = false; k.resetAt = 0; });
  activeKeyIdx = 0;
  console.log('♻️  All Gemini keys reset');
}

// reset exhausted keys every 65 seconds
setInterval(() => {
  const now = Date.now();
  let anyReset = false;
  keyStats.forEach(k => {
    if (k.exhausted && k.resetAt > 0 && now > k.resetAt) {
      k.exhausted = false;
      k.resetAt = 0;
      anyReset = true;
      console.log(`♻️  Key ${k.i + 1} quota reset — back in rotation`);
    }
  });
  // If active key is exhausted after reset, move to a fresh one
  if (anyReset && keyStats[activeKeyIdx].exhausted) rotateGeminiKey();
}, 65000);

async function callGemini(prompt) {
  let tried = 0;
  while (tried < GEMINI_KEYS.length) {
    try {
      const g = new GoogleGenerativeAI(GEMINI_KEYS[activeKeyIdx]);
      const m = g.getGenerativeModel({ model: 'gemini-2.0-flash' });
      keyStats[activeKeyIdx].calls++;
      const res = await m.generateContent(prompt);
      const text = res.response.text().trim();
      if (!text) throw new Error('Empty Gemini response');
      return text;
    } catch (e) {
      const msg = e.message || '';
      keyStats[activeKeyIdx].errors++;
      if (msg.includes('429') || msg.includes('RESOURCE_EXHAUSTED') || msg.includes('quota') || msg.includes('Too Many')) {
        console.log(`⚠️  Key ${activeKeyIdx + 1} quota hit`);
        keyStats[activeKeyIdx].exhausted = true;
        keyStats[activeKeyIdx].resetAt = Date.now() + 65000;
        rotateGeminiKey();
        tried++;
        await sleep(500);
        continue;
      }
      // Non-quota error — log and return null (don't crash)
      console.error('Gemini error (non-quota):', msg.slice(0, 120));
      return null;
    }
  }
  console.error('All Gemini keys exhausted');
  return null;
}

async function callGeminiJSON(prompt, fallback = {}) {
  const raw = await callGemini(prompt);
  if (!raw) return fallback;
  try {
    return JSON.parse(raw.replace(/```json|```/g, '').trim());
  } catch (e) {
    console.error('Gemini JSON parse error:', e.message, '| raw:', raw.slice(0, 200));
    return fallback;
  }
}

// ── TWILIO ──────────────────────────────────────────────────────
const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const FROM_NUMBER  = `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`;

function toWA(raw) {
  if (raw.startsWith('whatsapp:')) return raw;
  const d = raw.replace(/[^0-9]/g, '');
  return `whatsapp:+${d.startsWith('91') ? d : '91' + d}`;
}

async function sendText(to, body) {
  if (!body || !body.trim()) return;
  try {
    await twilioClient.messages.create({ from: FROM_NUMBER, to: toWA(to), body });
  } catch (e) {
    console.error('sendText FAILED:', e.message.slice(0, 100));
  }
}

async function sendImage(to, imageUrl, caption) {
  try {
    await twilioClient.messages.create({ from: FROM_NUMBER, to: toWA(to), body: caption, mediaUrl: [imageUrl] });
  } catch (e) {
    await sendText(to, caption); // fallback
  }
}

// ── STATE ────────────────────────────────────────────────────────
const pendingFollowUp = new Map(); // phone → timer
const pendingOrders   = new Map(); // phone → { timer, items }
// conversation context: stores last 30 min result per customer
const ctxStore = new Map(); // phone → { availableItems, notAvailableItems, lastProducts, ts }

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── DB TABLES SETUP ─────────────────────────────────────────────
async function ensureTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_interactions (
        id SERIAL PRIMARY KEY, customer_id INTEGER,
        phone_number VARCHAR(20), message TEXT, intent VARCHAR(50),
        category VARCHAR(50), bot_response TEXT, created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS customer_orders (
        order_id SERIAL PRIMARY KEY, customer_id INTEGER,
        phone_number VARCHAR(20), product_name TEXT,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS order_bans (
        ban_id SERIAL PRIMARY KEY, customer_id INTEGER,
        phone_number VARCHAR(20), ban_until TIMESTAMP,
        reason TEXT, created_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ DB tables ready');
  } catch(e) { console.error('Table setup error:', e.message); }
}

// ── DB HELPERS ───────────────────────────────────────────────────
async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g, '').replace(/^91/, '');
  try {
    const r = await pool.query(`SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`, [`%${clean}%`]);
    return r.rows[0] || null;
  } catch(e) { console.error('getCustomerByPhone error:', e.message); return null; }
}

async function isNewCustomer(id) {
  try {
    const r = await pool.query(`SELECT COUNT(*) c FROM customer_interactions WHERE customer_id=$1`, [id]);
    return parseInt(r.rows[0].c) === 0;
  } catch(e) { return false; }
}

async function getPreferences(id) {
  try {
    const r = await pool.query(`SELECT category, preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`, [id]);
    return r.rows;
  } catch(e) { return []; }
}

async function getPreferredProducts(customerId, limit = 4) {
  try {
    const r = await pool.query(`
      SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
        COALESCE(o.discount_percent,0) as discount_percent,
        ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
      FROM products p
      JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
      LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
      WHERE p.is_available=true AND p.stock_quantity>0
      ORDER BY cp.preference_score DESC, COALESCE(o.discount_percent,0) DESC LIMIT $2`,
      [customerId, limit]);
    return r.rows;
  } catch(e) { return []; }
}

async function getOffers(customerId) {
  try {
    const r = await pool.query(`
      SELECT p.name, p.brand, p.price, p.image_url, o.discount_percent,
        ROUND(p.price*(1-o.discount_percent/100.0),2) as offer_price,
        ROUND(p.price*o.discount_percent/100.0,2) as you_save, p.category,
        COALESCE(cp.preference_score,0) as relevance
      FROM offers o JOIN products p ON p.product_id=o.product_id
      LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
      WHERE o.valid_till>NOW() AND p.is_available=true AND p.stock_quantity>0
      ORDER BY relevance DESC, o.discount_percent DESC LIMIT 6`, [customerId]);
    return r.rows;
  } catch(e) { return []; }
}

// Search products by specific terms (name/brand/category match)
async function searchProducts(terms) {
  const validTerms = (terms || []).filter(t => t && t.trim().length > 0);
  if (validTerms.length === 0) return [];
  try {
    const conditions = validTerms.map((_,i) =>
      `(LOWER(p.name) LIKE LOWER($${i+1}) OR LOWER(COALESCE(p.brand,'')) LIKE LOWER($${i+1}) OR LOWER(p.category) LIKE LOWER($${i+1}))`
    ).join(' OR ');
    const r = await pool.query(`
      SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
        COALESCE(o.discount_percent,0) as discount_percent,
        ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
      FROM products p
      LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
      WHERE p.is_available=true AND p.stock_quantity>0 AND (${conditions})
      ORDER BY COALESCE(o.discount_percent,0) DESC, p.price ASC`,
      validTerms.map(t => `%${t.trim()}%`));
    const seen = new Set();
    return r.rows.filter(p => { if (seen.has(p.product_id)) return false; seen.add(p.product_id); return true; });
  } catch(e) { console.error('searchProducts error:', e.message); return []; }
}

// Get the FULL product catalog — given to Gemini so it can actually understand what we have
async function getAllProducts() {
  try {
    const r = await pool.query(`
      SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
        COALESCE(o.discount_percent,0) as discount_percent,
        ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
      FROM products p
      LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
      WHERE p.is_available=true AND p.stock_quantity>0
      ORDER BY p.category, p.name`);
    return r.rows;
  } catch(e) { console.error('getAllProducts error:', e.message); return []; }
}

async function addProductToDB(name, category, brand, price) {
  try {
    await pool.query(`
      INSERT INTO products(name,category,brand,price,cost_price,stock_quantity,reorder_threshold,is_available,image_url)
      VALUES($1,$2,$3,$4,$5,100,10,true,'') ON CONFLICT DO NOTHING`,
      [name, category, brand || '', price, Math.round(price * 0.7)]);
    console.log('🆕 Added to DB:', name);
  } catch(e) { console.error('addProduct error:', e.message); }
}

async function logInteraction(customerId, phone, message, intent, category, reply) {
  try {
    await pool.query(
      `INSERT INTO customer_interactions(customer_id,phone_number,message,intent,category,bot_response) VALUES($1,$2,$3,$4,$5,$6)`,
      [customerId, phone, message, intent, category || null, (reply||'').slice(0,500)]);
  } catch(e) {}
}

// ── PRODUCT CARD ──────────────────────────────────────────────
async function sendProductCard(to, p, isFirst = false) {
  let cap = isFirst ? '⭐ *TOP PICK*\n' : '';
  cap += `*${p.name}*`;
  if (p.brand) cap += ` — ${p.brand}`;
  cap += `\n📦 ${p.category}\n`;
  const disc = parseFloat(p.discount_percent || 0);
  if (disc > 0) {
    const fin = p.final_price || p.offer_price || p.price;
    const save = (parseFloat(p.price) - parseFloat(fin)).toFixed(0);
    cap += `~~Rs.${p.price}~~ → *Rs.${fin}* 🏷️ *${disc}% OFF*\n💰 You save Rs.${save}`;
  } else {
    cap += `💵 *Rs.${p.price}*`;
  }
  cap += `\n\n📍 Available at Dmart — grab it fresh!\nReply *"order ${p.name}"* to note your pickup!`;
  if (p.image_url && p.image_url.startsWith('http')) {
    await sendImage(to, p.image_url, cap);
  } else {
    await sendText(to, cap);
  }
}

// ── LOCAL INTENT DETECTION (no Gemini needed) ─────────────────
const GREET_WORDS = new Set(['hi','hii','hiii','hello','hey','helo','hlo','yo','sup','hai','vanakkam','namaste','good morning','good evening','good afternoon','goodmorning','goodevening']);
const OFFER_WORDS = ['offer','offers','deal','deals','discount','discounts','sale','savings','best price','any offer','any deal'];
const ORDER_WORDS = ['order it','pack it',"i'll pick",'ill pick','book it','pack them','order those','order all','pack all','confirm order','yes order','pack available','order available','place order','order and pick'];
const DMART_Q    = ['store time','opening time','closing time','timings','open at','return policy','return item','exchange','parking','dmart ready','dmart app','store location','nearest dmart'];

function detectLocalIntent(msg) {
  const m = msg.toLowerCase().trim();
  const mClean = m.replace(/[!.?]+$/,'');

  // Pure greeting ONLY — if message has more words after the greeting, let Gemini handle it
  const words = m.split(/\s+/);
  const firstWord = words[0].replace(/[!.?]+$/,'');
  const isPureGreeting = GREET_WORDS.has(mClean) || (GREET_WORDS.has(firstWord) && words.length <= 2 && !words.slice(1).some(w => w.length > 3));
  if (isPureGreeting) return 'greeting';

  if (OFFER_WORDS.some(w => m.includes(w))) return 'offers';
  if (ORDER_WORDS.some(w => m.includes(w))) return 'order';
  if (m.includes('what can you') || m.includes('how can you') || m.includes('who are you') || m.includes('what do you do')) return 'capabilities';
  if (DMART_Q.some(w => m.includes(w))) return 'dmart_question';
  return null;
}

function getDmartAnswer(m) {
  if (m.includes('time') || m.includes('open') || m.includes('close') || m.includes('timing'))
    return `🕐 *Dmart Store Timings:*\nMon–Sun: 8:00 AM – 10:00 PM\n\n_(Timings may vary by location — confirm with your local store!)_`;
  if (m.includes('return') || m.includes('exchange'))
    return `🔄 *Dmart Return Policy:*\nMost items returnable within 30 days with the original bill. Fresh produce, food items, and undergarments are non-returnable. Bring the item + bill to the store!`;
  if (m.includes('parking'))
    return `🚗 *Parking at Dmart:*\nMost Dmart stores have free parking for 2-wheelers and 4-wheelers. Check your specific store for details!`;
  if (m.includes('ready') || m.includes('app'))
    return `📱 *Dmart Ready App:*\nSearch "DMart Ready" on Play Store or App Store for online delivery. But honestly — walking in gets you fresher products, same low prices, and no delivery wait! 😊`;
  return null;
}

// ── WELCOME ───────────────────────────────────────────────────
function buildWelcome(name) {
  return `👋 *Hey ${name}! Welcome to your Dmart Assistant!* 🛒

I'm your personal shopping friend here. Here's everything I can do for you:

🔍 *Find any product* — "Show me snacks" or "Do you have Lays?"
📋 *Check your shopping list* — Paste your full list, I'll check availability, prices and offers
🏷️ *Show live deals* — "What offers today?"
📦 *Note your pickup order* — "Pack it, I'll pick it up"
💡 *Answer Dmart questions* — store timings, return policy, app — anything!
🤝 *Personalized picks* — I know what you like and find the best deals for you

I'm here 24/7. What do you need today? 😊`;
}

// ── FOLLOW-UP (60s after welcome if no reply) ────────────────
async function sendFollowUp(to, customer) {
  try {
    const prods = await getPreferredProducts(customer.customer_id, 3);
    if (prods.length === 0) {
      await sendText(to, `🛒 *${customer.name}!* Still there? Just ask for any product or paste your shopping list — I'm ready! 😊`);
      return;
    }
    const h = prods.find(p => parseFloat(p.discount_percent) > 0) || prods[0];
    await sendText(to,
      `🤔 *${customer.name}, still thinking?*\n\n` +
      `I know what you love — *${h.name}* ${parseFloat(h.discount_percent) > 0
        ? `is at *${h.discount_percent}% OFF* right now! Only Rs.${h.final_price} (was Rs.${h.price})`
        : `is in stock at Rs.${h.price}`}.\n\n` +
      `Fresh products, real prices. No delivery fee, no waiting. Just walk in! 😊\n\nWhat do you need today?`
    );
    await sleep(600);
    for (let i = 0; i < Math.min(prods.length, 2); i++) {
      await sendProductCard(to, prods[i], i === 0);
      await sleep(500);
    }
  } catch(e) { console.error('sendFollowUp error:', e.message); }
}

// ── ORDER SYSTEM ─────────────────────────────────────────────
async function isCustomerBanned(customerId, phone) {
  try {
    const r = await pool.query(`SELECT * FROM order_bans WHERE (customer_id=$1 OR phone_number=$2) AND ban_until>NOW() LIMIT 1`, [customerId, phone]);
    return r.rows[0] || null;
  } catch(e) { return null; }
}

async function getCancelledCount(customerId) {
  try {
    const r = await pool.query(`SELECT COUNT(*) c FROM customer_orders WHERE customer_id=$1 AND status='cancelled' AND created_at>NOW()-INTERVAL '30 days'`, [customerId]);
    return parseInt(r.rows[0].c);
  } catch(e) { return 0; }
}

async function handleOrder(to, customer, itemList) {
  const phone = to.replace('whatsapp:','').replace('+','');
  const items = Array.isArray(itemList) ? itemList.filter(Boolean) : [String(itemList)];
  if (items.length === 0) { await sendText(to, `Which item do you want to order ${customer.name}? 😊`); return; }

  const ban = await isCustomerBanned(customer.customer_id, phone);
  if (ban) {
    await sendText(to, `⚠️ *${customer.name}*, ordering is paused until *${new Date(ban.ban_until).toLocaleDateString('en-IN')}*.\n\nYou can still browse products! 🛒`);
    return;
  }
  const cancels = await getCancelledCount(customer.customer_id);
  if (cancels >= 3) {
    try { await pool.query(`INSERT INTO order_bans(customer_id,phone_number,ban_until,reason) VALUES($1,$2,NOW()+INTERVAL '30 days',$3)`, [customer.customer_id, phone, 'Too many no-shows']); } catch(e) {}
    await sendText(to, `⚠️ *${customer.name}*, ordering paused for 30 days.\n\n3 orders weren't picked up. Paused to keep things fair for everyone. You can still browse! 😊`);
    return;
  }

  // Clear previous order timer if any
  if (pendingOrders.has(phone)) { clearTimeout(pendingOrders.get(phone).timer); }

  const orderIds = [];
  for (const item of items.slice(0, 6)) {
    try {
      const r = await pool.query(`INSERT INTO customer_orders(customer_id,phone_number,product_name,status) VALUES($1,$2,$3,'pending') RETURNING order_id`, [customer.customer_id, phone, item]);
      orderIds.push(r.rows[0].order_id);
    } catch(e) {}
  }

  await sendText(to,
    `✅ *Got it ${customer.name}!*\n\n` +
    `Order noted for:\n${items.map(i=>`• ${i}`).join('\n')}\n\n` +
    `⏳ Give us *3 minutes* to prepare!\n\n` +
    `📍 Please have payment ready when you arrive.\n_(Pay at the counter — super easy!)_ 😊`
  );

  const readyTimer = setTimeout(async () => {
    try {
      for (const oid of orderIds) await pool.query(`UPDATE customer_orders SET status='ready',updated_at=NOW() WHERE order_id=$1`,[oid]);
      await sendText(to,
        `🎉 *${customer.name}, your order is READY!*\n\n` +
        `✅ Packed and waiting:\n${items.map(i=>`• ${i}`).join('\n')}\n\n` +
        `📍 Come pick it up anytime. Payment at the counter! 😊`
      );
      // 30 min no-show timer
      const noShowTimer = setTimeout(async () => {
        try {
          for (const oid of orderIds) await pool.query(`UPDATE customer_orders SET status='cancelled',updated_at=NOW() WHERE order_id=$1`,[oid]);
          const count = await getCancelledCount(customer.customer_id);
          await sendText(to,
            `⚠️ *Order cancelled — ${customer.name}*\n\n` +
            `We waited but you didn't come, so the order was cancelled. No worries, order again anytime!\n\n` +
            `_(${count}/3 cancellations — after 3, ordering pauses for 1 month to keep things fair.)_`
          );
          pendingOrders.delete(phone);
        } catch(e) {}
      }, 30*60*1000);
      pendingOrders.set(phone, { timer: noShowTimer, stage: 'ready' });
    } catch(e) {}
  }, 3*60*1000);

  pendingOrders.set(phone, { timer: readyTimer, stage: 'pending' });
}

// ── GEMINI BRAIN ─────────────────────────────────────────────
async function geminiThink(message, customer, prefs, allProducts, ctx) {
  const prefList = prefs.map(p=>p.category).join(', ') || 'General';

  // Build full catalog string so Gemini knows every product we have
  const catalog = allProducts.map(p => {
    const d = parseFloat(p.discount_percent||0);
    return d > 0
      ? `${p.name}|${p.category}|${p.brand||''}|Rs${p.price}→Rs${p.final_price}(${d}%OFF)`
      : `${p.name}|${p.category}|${p.brand||''}|Rs${p.price}`;
  }).join('\n');

  const ctxPart = ctx && (Date.now()-ctx.ts) < 30*60*1000 ? `
WHAT HAPPENED BEFORE (${Math.round((Date.now()-ctx.ts)/60000)} mins ago):
  Items found: ${(ctx.availableItems||[]).join(', ')||'none'}
  Items not found: ${(ctx.notAvailableItems||[]).join(', ')||'none'}
  Products shown: ${(ctx.lastProducts||[]).slice(0,3).map(p=>p.name).join(', ')||'none'}
→ If customer's new message is replying to this (like "okay pack available", "order those", "show me those again") — set intent to follow_up.` : '';

  const prompt = `You are the Dmart India WhatsApp shopping assistant. You think like a smart human friend who works at Dmart.

CUSTOMER: ${customer.name}
CUSTOMER LIKES: ${prefList}
CUSTOMER SAYS: "${message}"
${ctxPart}

OUR FULL PRODUCT CATALOG (name|category|brand|price):
${catalog || 'No products available'}

YOUR JOB:
1. READ THE FULL MESSAGE — understand what the customer ACTUALLY means, not just keywords
2. MATCH their request intelligently to our catalog above
3. If a product is in the catalog → put it in matched_products
4. If a product is NOT in the catalog → put it in unknown_items (we will check internet for those)

Reply ONLY with valid JSON — absolutely no markdown, no text outside JSON:
{
  "intent": "greeting|shopping_list|search|browse_category|offers|order|follow_up|question|out_of_scope",
  "reply": "warm 1-2 sentence reply as a caring shopping friend. For product requests: subtly mention Dmart = real prices, no delivery markup. Sound natural.",
  "shopping_list_items": [],
  "matched_products": [],
  "search_terms": [],
  "unknown_items": [],
  "order_items": [],
  "follow_up_action": null,
  "dmart_answer": null
}

INTENT RULES — read all of these carefully:
- greeting: ONLY if the message is just a greeting with no product request ("hi", "hello", "good morning")
- shopping_list: customer gives a LIST of 2+ items to check — strip quantities from item names, e.g. "Beans 1kg" → "Beans", "Lays 20rs 5 packets" → "Lays". Put each item in shopping_list_items[].
- search: asking about 1-2 specific products by name — e.g. "do you have Lays?", "show me biscuits"
- browse_category: asking to see a product category — e.g. "show me snacks", "what dairy products", "show all fruits", "check and tell me snacks items"
- offers: asking about deals/discounts/offers
- order: says order/pack it/I'll pick up/book it — extract product name(s) in order_items[]
- follow_up: customer responds to previous result — "okay pack available ones", "order those", "show me those products again", "except unavailable pack the rest"
- question: asks about bot capabilities ("what can you do?"), store timings, return policy, parking, Dmart app — put your full 2-sentence answer in dmart_answer
- out_of_scope: cricket, movies, weather, politics — nothing to do with shopping

CATALOG MATCHING RULES:
- "Okay check and tell me snacks items" → intent: browse_category, search_terms: ["Snacks"]
- "Hi show me snacks" → intent: browse_category (the "hi" is just greeting before the request)
- "What are the things you can do?" → intent: question, explain bot capabilities in dmart_answer
- When matching: "Lays" can match "Lays Classic" from catalog. Be smart about partial matches.
- If a product name from catalog clearly matches what customer wants → put that exact catalog name in matched_products
- Items NOT anywhere in catalog → put in unknown_items so we can check the internet for them`;

  const fallback = {
    intent:'search', reply:`On it ${customer.name}! 😊`,
    shopping_list_items:[], matched_products:[], search_terms:[message.split(' ').find(w=>w.length>3)||message],
    unknown_items:[], order_items:[], follow_up_action:null, dmart_answer:null
  };

  const result = await callGeminiJSON(prompt, fallback);
  result.shopping_list_items = result.shopping_list_items || [];
  result.matched_products    = result.matched_products    || [];
  result.search_terms        = result.search_terms        || [];
  result.unknown_items       = result.unknown_items       || [];
  result.order_items         = result.order_items         || [];
  result.reply               = result.reply               || fallback.reply;
  console.log(`🧠 Intent:${result.intent} | Matched:${result.matched_products.length} | List:${result.shopping_list_items.length} | Unknown:${result.unknown_items.length} | Search:${result.search_terms.join(',')}`);
  return result;
}

// check unknown items with Gemini (all in one call)
async function checkUnknownItems(items) {
  if (!items || items.length === 0) return [];
  const prompt = `Dmart India product expert. Check which of these are sold at Dmart India stores.
Items: ${items.join(', ')}
Dmart sells: groceries, FMCG, clothing, footwear, home goods. NOT electronics or medicines.
Reply ONLY valid JSON array no markdown:
[{"item":"original name","available":true,"dmart_name":"exact Dmart product name","category":"category","brand":"brand or empty","price":number_in_rupees}]
Not at Dmart: {"item":"name","available":false,"dmart_name":"","category":"","brand":"","price":0}`;

  const result = await callGeminiJSON(prompt, []);
  return Array.isArray(result) ? result : [];
}

// ── SHOPPING LIST HANDLER ────────────────────────────────────
async function handleShoppingList(to, customer, items, reply) {
  await sendText(to, `${reply}\n\n📋 *Checking your ${items.length} items at Dmart...*`);

  const dbProducts = await searchProducts(items);
  const availableItems = [];
  const foundProducts  = [];
  const unknownItems   = [];

  for (const item of items) {
    const kl = item.toLowerCase();
    const match = dbProducts.find(p =>
      p.name.toLowerCase().includes(kl) ||
      kl.includes(p.name.toLowerCase().split(' ')[0]) ||
      (p.brand && p.brand.toLowerCase().includes(kl)) ||
      p.category.toLowerCase().includes(kl)
    );
    if (match) {
      availableItems.push(item);
      if (!foundProducts.find(f=>f.product_id===match.product_id)) foundProducts.push(match);
    } else {
      unknownItems.push(item);
    }
  }

  // Check unknowns with Gemini (1 call for all)
  const geminiResults = unknownItems.length > 0 ? await checkUnknownItems(unknownItems) : [];
  const notAvailableItems = [];

  for (const gr of geminiResults) {
    if (gr.available && gr.dmart_name) {
      availableItems.push(gr.item);
      await addProductToDB(gr.dmart_name, gr.category, gr.brand||'', gr.price);
      foundProducts.push({ name:gr.dmart_name, category:gr.category, brand:gr.brand||'', price:gr.price, discount_percent:0, final_price:gr.price, image_url:'' });
    } else {
      notAvailableItems.push(gr.item);
    }
  }
  // items Gemini didn't return a result for
  for (const item of unknownItems) {
    if (!geminiResults.find(g=>g.item&&g.item.toLowerCase()===item.toLowerCase())) {
      notAvailableItems.push(item);
    }
  }

  await sleep(500);

  // Build report
  let report = `📋 *Shopping List — ${customer.name}:*\n\n`;
  if (availableItems.length > 0) {
    report += `✅ *Available at Dmart (${availableItems.length}/${items.length}):*\n`;
    for (const p of foundProducts) {
      const d = parseFloat(p.discount_percent||0);
      report += `• *${p.name}* — Rs.${d>0?p.final_price+` *(${d}% OFF)*`:p.price}\n`;
    }
  }
  if (notAvailableItems.length > 0) {
    report += `\n❌ *Not available at Dmart (${notAvailableItems.length}):*\n`;
    report += notAvailableItems.map(i=>`• ${i}`).join('\n');
  }

  await sendText(to, report);
  await sleep(600);

  // Send product cards (with images first, max 3)
  const withImg = foundProducts.filter(p=>p.image_url&&p.image_url.startsWith('http'));
  for (let i = 0; i < Math.min(withImg.length, 3); i++) {
    await sendProductCard(to, withImg[i], i===0);
    await sleep(600);
  }

  const totalSave = foundProducts.reduce((acc,p)=>{
    const d=parseFloat(p.discount_percent||0);
    return acc+(d>0?parseFloat(p.price)-parseFloat(p.final_price):0);
  },0);

  await sendText(to,
    `🛒 *${availableItems.length} items ready at Dmart!*` +
    (totalSave>0?` Save Rs.${totalSave.toFixed(0)} with active offers!`:'') +
    `\n\nJust walk in — everything's fresh. No delivery wait, no extra fees! 😊\n\n` +
    `Say *"pack it, I'll pick it up"* to note your order! 📦`
  );

  return { availableItems, notAvailableItems, foundProducts };
}

// ── MAIN MESSAGE PROCESSOR ───────────────────────────────────
async function processMessage(fromRaw, rawMessage) {
  const message = rawMessage.trim();
  const phone = fromRaw.replace('whatsapp:','').replace('+','');
  console.log(`\n═══ MSG from ${phone}: "${message.slice(0,80)}"`);

  // Cancel pending follow-up — customer responded
  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
  }

  const customer = await getCustomerByPhone(phone);
  if (!customer) {
    await sendText(fromRaw, `👋 *Welcome to Dmart Assistant!*\n\nYour number isn't registered yet. Visit Dmart to register and unlock personalized shopping!\n\nYou can still ask about any product. 🛒`);
    return;
  }
  console.log(`👤 ${customer.name}`);

  // ── NEW CUSTOMER ──
  const isNew = await isNewCustomer(customer.customer_id);
  if (isNew) {
    await sendText(fromRaw, buildWelcome(customer.name));
    await logInteraction(customer.customer_id, phone, message, 'welcome', null, 'welcome sent');
    const t = setTimeout(async()=>{ try { await sendFollowUp(fromRaw,customer); pendingFollowUp.delete(phone); } catch(e){} }, 60000);
    pendingFollowUp.set(phone, t);
    return;
  }

  const ctx = ctxStore.get(phone) || null;

  // ── LOCAL INTENT (no Gemini) ──
  const localIntent = detectLocalIntent(message);

  if (localIntent === 'greeting') {
    const offers = await getOffers(customer.customer_id);
    await sendText(fromRaw,
      `Hey ${customer.name}! 😊 Great to hear from you!\n\n` +
      (offers.length>0?`🔥 *${offers.length} deals* live right now — ask *"show offers"* to see them!\n\n`:'')+
      `What do you need today?`
    );
    await logInteraction(customer.customer_id, phone, message, 'greeting', null, 'local');
    return;
  }

  if (localIntent === 'capabilities') {
    await sendText(fromRaw,
      `🛒 *I'm your Dmart Assistant! Here's what I can do:*\n\n` +
      `🔍 *Find any product* — "Show snacks" / "Do you have Lays?"\n` +
      `📋 *Check your shopping list* — Paste it and I'll check every item\n` +
      `🏷️ *Show live deals* — "What offers today?"\n` +
      `📦 *Note your pickup order* — "Pack it, I'll pick it up"\n` +
      `💡 *Answer Dmart questions* — timings, return policy, app, parking\n` +
      `🤝 *Personal picks* — I know what you like!\n\nWhat do you need? 😊`
    );
    await logInteraction(customer.customer_id, phone, message, 'question', null, 'capabilities');
    return;
  }

  if (localIntent === 'dmart_question') {
    const ans = getDmartAnswer(message.toLowerCase()) || `I'd recommend checking with your nearest Dmart store for the most accurate info! 😊`;
    await sendText(fromRaw, `💡 ${ans}`);
    await logInteraction(customer.customer_id, phone, message, 'question', null, ans.slice(0,100));
    return;
  }

  if (localIntent === 'offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length === 0) {
      await sendText(fromRaw, `No active offers right now ${customer.name}! Want to browse products? 😊`);
      return;
    }
    await sendText(fromRaw, `🔥 *${customer.name}, here are today's best deals — including things you love!*`);
    await sleep(600);
    for (let i = 0; i < Math.min(offers.length, 5); i++) {
      const o = offers[i];
      const cap = `🏷️ *${o.name}*${o.brand?' — '+o.brand:''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n💰 Save Rs.${o.you_save}!\n\n📍 In stock at Dmart — grab it yourself, no delivery fee! 😊`;
      if (o.image_url&&o.image_url.startsWith('http')) await sendImage(fromRaw, o.image_url, cap);
      else await sendText(fromRaw, cap);
      await sleep(600);
    }
    await sendText(fromRaw, `Want to check your shopping list? Just paste it! 📋`);
    await logInteraction(customer.customer_id, phone, message, 'offers', null, 'local offers');
    return;
  }

  if (localIntent === 'order') {
    // Order from context if available
    if (ctx && ctx.availableItems && ctx.availableItems.length > 0) {
      await handleOrder(fromRaw, customer, ctx.availableItems);
      await logInteraction(customer.customer_id, phone, message, 'order', null, 'from context');
      return;
    }
    // No context — fall through to Gemini to figure out what they want to order
  }

  // ── GEMINI NEEDED ──
  const prefs = await getPreferences(customer.customer_id);
  const allProducts = await getAllProducts(); // full catalog — Gemini needs this to actually understand requests
  const brain = await geminiThink(message, customer, prefs, allProducts, ctx);

  // ── GREETING (Gemini) ──
  if (brain.intent === 'greeting') {
    const offers = await getOffers(customer.customer_id);
    await sendText(fromRaw,
      `${brain.reply}\n\n`+
      (offers.length>0?`🔥 *${offers.length} deals* live right now — ask *"show offers"* to see them!\n\n`:'')+
      `What do you need today? 😊`
    );
    await logInteraction(customer.customer_id, phone, message, 'greeting', null, 'gemini');
    return;
  }

  // ── OUT OF SCOPE ──
  if (brain.intent === 'out_of_scope') {
    await sendText(fromRaw,
      `😄 Ha! I wish I could help with that ${customer.name}!\n\nBut I'm your *Dmart shopping friend* — only good at products and deals! 🛒\n\nTry: "Show snacks" or "What offers today?" or just paste your shopping list!`
    );
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, 'gemini');
    // Schedule follow-up
    const t = setTimeout(async()=>{ try { await sendFollowUp(fromRaw,customer); pendingFollowUp.delete(phone); } catch(e){} }, 60000);
    pendingFollowUp.set(phone, t);
    return;
  }

  // ── DMART QUESTION (Gemini) ──
  if (brain.intent === 'question' && brain.dmart_answer) {
    await sendText(fromRaw, `💡 ${brain.dmart_answer}\n\nAnything else? 😊`);
    await logInteraction(customer.customer_id, phone, message, 'question', null, brain.dmart_answer.slice(0,100));
    return;
  }

  // ── FOLLOW-UP (context-aware reply) ──
  if (brain.intent === 'follow_up' && ctx && ctx.availableItems && ctx.availableItems.length > 0) {
    if (brain.follow_up_action === 'order_available') {
      await handleOrder(fromRaw, customer, ctx.availableItems);
      await logInteraction(customer.customer_id, phone, message, 'follow_up_order', null, ctx.availableItems.join(',').slice(0,100));
    } else {
      // Show available items again
      await sendText(fromRaw,
        `${brain.reply}\n\n✅ *Available items from your last list:*\n\n${ctx.availableItems.map(i=>`• ${i}`).join('\n')}\n\nSay *"pack it, I'll pick it up"* to order all of them! 📦`
      );
      await sleep(600);
      const withImg = (ctx.lastProducts||[]).filter(p=>p.image_url&&p.image_url.startsWith('http')).slice(0,2);
      for (let i=0; i<withImg.length; i++) { await sendProductCard(fromRaw, withImg[i], i===0); await sleep(500); }
      await logInteraction(customer.customer_id, phone, message, 'follow_up_show', null, 'shown available');
    }
    return;
  }

  // ── ORDER (Gemini) ──
  if (brain.intent === 'order') {
    const items = brain.order_items.length > 0 ? brain.order_items :
                  brain.matched_products.length > 0 ? brain.matched_products :
                  ctx && ctx.availableItems ? ctx.availableItems : [];
    if (items.length === 0) {
      await sendText(fromRaw, `${brain.reply}\n\nWhich item would you like to order? 😊`);
    } else {
      await handleOrder(fromRaw, customer, items);
    }
    await logInteraction(customer.customer_id, phone, message, 'order', null, items.join(',').slice(0,100));
    return;
  }

  // ── SHOPPING LIST ──
  if (brain.intent === 'shopping_list' && brain.shopping_list_items.length >= 2) {
    const result = await handleShoppingList(fromRaw, customer, brain.shopping_list_items, brain.reply);
    ctxStore.set(phone, { availableItems:result.availableItems, notAvailableItems:result.notAvailableItems, lastProducts:result.foundProducts, ts:Date.now() });
    setTimeout(() => ctxStore.delete(phone), 30*60*1000);
    await logInteraction(customer.customer_id, phone, message, 'shopping_list', null, `${result.availableItems.length}/${brain.shopping_list_items.length}`);
    return;
  }

  // ── SEARCH / BROWSE CATEGORY ──
  const searchTerms = [...new Set([...brain.matched_products, ...brain.search_terms])].filter(Boolean);
  let dbResults = searchTerms.length > 0 ? await searchProducts(searchTerms) : [];

  if (dbResults.length > 0) {
    await sendText(fromRaw, `${brain.reply}\n\n🛒 *Found ${dbResults.length} product${dbResults.length>1?'s':''} at Dmart:*`);
    await sleep(600);
    for (let i=0; i<Math.min(dbResults.length,5); i++) {
      await sendProductCard(fromRaw, dbResults[i], i===0);
      await sleep(600);
    }
    const offCnt = dbResults.filter(p=>parseFloat(p.discount_percent)>0).length;
    await sendText(fromRaw,
      (offCnt>0?`🎉 *${offCnt} of these have active offers!*\n\n`:'')+
      `📍 All in stock — just walk in. No delivery wait, no extra fees! 🛒\n\nNeed more? Send your full list! 📋`
    );
    ctxStore.set(phone, { availableItems:dbResults.map(p=>p.name), notAvailableItems:brain.unknown_items||[], lastProducts:dbResults, ts:Date.now() });
    setTimeout(()=>ctxStore.delete(phone), 30*60*1000);

  } else if (brain.unknown_items && brain.unknown_items.length > 0) {
    console.log('No DB match — checking Gemini for:', brain.unknown_items);
    const geminiResults = await checkUnknownItems(brain.unknown_items);
    const found = geminiResults.filter(g=>g.available&&g.dmart_name);
    const notFound = geminiResults.filter(g=>!g.available).map(g=>g.item);

    if (found.length > 0) {
      for (const g of found) await addProductToDB(g.dmart_name, g.category, g.brand||'', g.price);
      await sendText(fromRaw, `${brain.reply}\n\n✅ *Found at Dmart:*`);
      await sleep(400);
      for (const g of found) {
        await sendText(fromRaw, `*${g.dmart_name}*${g.brand?' — '+g.brand:''}\n📦 ${g.category}\n💵 *Rs.${g.price}*\n\n📍 Available at Dmart — pick it up and save on delivery! 😊`);
        await sleep(500);
      }
      if (notFound.length>0) await sendText(fromRaw, `😕 *Not available at Dmart:*\n${notFound.map(i=>`• ${i}`).join('\n')}`);
    } else {
      await sendText(fromRaw,
        `${brain.reply}\n\n😕 *Couldn't find that at Dmart.*\n\nTry:\n🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🌾 Grains | 👟 Footwear`
      );
    }
  } else {
    await sendText(fromRaw,
      `${brain.reply}\n\n😕 *Nothing matched in our store.*\n\nTry browsing:\n🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🌾 Grains`
    );
  }

  await logInteraction(customer.customer_id, phone, message, brain.intent, (brain.search_terms||[])[0]||null, 'replied');
}

// ── ENDPOINTS ────────────────────────────────────────────────
app.post('/whatsapp', (req, res) => {
  // Always respond to Twilio INSTANTLY — prevents 15s timeout
  res.set('Content-Type','text/xml');
  res.send('<Response></Response>');

  const fromRaw = (req.body.From||'').trim();
  const message = (req.body.Body||'').trim();
  console.log(`\n📩 WEBHOOK — From:${fromRaw} | Msg:${message.slice(0,60)}`);

  if (!fromRaw || !message) {
    console.log('Empty webhook — ignored');
    return;
  }

  processMessage(fromRaw, message).catch(e => {
    console.error('processMessage UNHANDLED ERROR:', e.message);
    console.error(e.stack);
    // Try to send an error message to the user
    sendText(fromRaw, `Sorry ${fromRaw}, something went wrong on our end. Please try again in a moment! 🙏`).catch(()=>{});
  });
});

app.get('/health', async (req, res) => {
  let dbOk = false;
  try { await pool.query('SELECT 1'); dbOk = true; } catch(e) {}
  res.json({
    status: 'ok',
    time: new Date().toISOString(),
    db: dbOk ? 'connected' : 'ERROR',
    gemini_keys: GEMINI_KEYS.length,
    active_key: activeKeyIdx + 1,
    keys_exhausted: keyStats.filter(k=>k.exhausted).length,
    total_calls: keyStats.reduce((a,k)=>a+k.calls,0)
  });
});

app.get('/keystatus', (req, res) => {
  res.json({
    total: GEMINI_KEYS.length,
    active: activeKeyIdx + 1,
    keys: keyStats.map(k => ({
      key: k.i+1, calls: k.calls, errors: k.errors,
      exhausted: k.exhausted,
      resets_in: k.exhausted ? Math.max(0, Math.round((k.resetAt-Date.now())/1000))+'s' : 'active'
    }))
  });
});

// ── START ────────────────────────────────────────────────────
ensureTables().then(() => {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`\n✅ Dmart Assistant LIVE on port ${PORT}`);
    console.log(`📍 Webhook: POST /whatsapp`);
    console.log(`🔑 ${GEMINI_KEYS.length} Gemini keys loaded`);
    console.log(`🏥 Health: GET /health\n`);
  });
});
