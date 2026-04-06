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

// ─────────────────────────────────────────
// SEND HELPERS
// ─────────────────────────────────────────
async function sendText(to, body) {
  let t = to;
  if (!t.startsWith('whatsapp:')) {
    const d = t.replace(/[^0-9]/g,'');
    t = `whatsapp:+${d.startsWith('91')?d:'91'+d}`;
  }
  await twilioClient.messages.create({ from:`whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`, to:t, body });
}

async function sendImage(to, imageUrl, caption) {
  let t = to;
  if (!t.startsWith('whatsapp:')) {
    const d = t.replace(/[^0-9]/g,'');
    t = `whatsapp:+${d.startsWith('91')?d:'91'+d}`;
  }
  try {
    await twilioClient.messages.create({ from:`whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`, to:t, body:caption, mediaUrl:[imageUrl] });
  } catch(e) {
    await twilioClient.messages.create({ from:`whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`, to:t, body:caption });
  }
}

async function sleep(ms) { return new Promise(r=>setTimeout(r,ms)); }

// ─────────────────────────────────────────
// DB HELPERS
// ─────────────────────────────────────────
async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g,'').replace(/^91/,'');
  const r = await pool.query(`SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`,[`%${clean}%`]);
  return r.rows[0]||null;
}

async function isNewCustomer(id) {
  const r = await pool.query(`SELECT COUNT(*) as c FROM customer_interactions WHERE customer_id=$1`,[id]);
  return parseInt(r.rows[0].c)===0;
}

async function getPreferences(id) {
  const r = await pool.query(`SELECT category, preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`,[id]);
  return r.rows;
}

// Get products by keyword search — searches name, category, brand
async function searchProducts(keyword, customerId, limit=6) {
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      COALESCE(o.discount_percent,0)*2 as score
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
      AND (LOWER(p.name) LIKE LOWER($1) OR LOWER(p.category) LIKE LOWER($1) OR LOWER(p.brand) LIKE LOWER($1))
    ORDER BY score DESC, p.price ASC LIMIT $2`,
    [`%${keyword}%`, limit]);
  return r.rows;
}

// Get preferred products for initial attraction
async function getPreferredProducts(customerId, limit=4) {
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      (cp.preference_score*6 + COALESCE(o.discount_percent,0)*4) as score
    FROM products p
    JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
    ORDER BY score DESC LIMIT $2`,[customerId,limit]);
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
    ORDER BY relevance DESC, o.discount_percent DESC LIMIT 8`,[customerId]);
  return r.rows;
}

async function checkProductInDB(name) {
  const r = await pool.query(`
    SELECT p.*, COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE LOWER(p.name) LIKE LOWER($1) AND p.is_available=true LIMIT 1`,[`%${name}%`]);
  return r.rows[0]||null;
}

async function addProductToDB(name, category, brand, price) {
  try {
    const r = await pool.query(`
      INSERT INTO products(name,category,brand,price,cost_price,stock_quantity,reorder_threshold,is_available,image_url)
      VALUES($1,$2,$3,$4,$5,100,10,true,'')
      ON CONFLICT DO NOTHING RETURNING product_id`,
      [name,category,brand||'',price,Math.round(price*0.7)]);
    return r.rows[0]?.product_id||null;
  } catch(e) { return null; }
}

async function logInteraction(customerId, phone, message, intent, category, reply) {
  try {
    await pool.query(`INSERT INTO customer_interactions(customer_id,phone_number,message,intent,category,bot_response) VALUES($1,$2,$3,$4,$5,$6)`,
      [customerId,phone,message,intent,category,reply]);
  } catch(e){}
}

// ─────────────────────────────────────────
// GEMINI — MASTER BRAIN
// Handles: intent, smart reply, product fetch, comparison, list check
// ─────────────────────────────────────────
async function callGemini(message, customerName, prefs, dbProductsFound) {
  const prefList = prefs.map(p=>p.category).join(', ')||'not set';
  const dbInfo = dbProductsFound.length>0
    ? `Products found in Dmart database: ${dbProductsFound.map(p=>`${p.name} Rs.${p.price}${p.discount_percent>0?' ('+p.discount_percent+'% OFF)':''}`).join(', ')}`
    : 'No exact products found in database for this query';

  const prompt = `You are "Dmart Assistant" — a super smart, friendly, proactive AI shopping friend for Dmart supermarket India.
You know everything about Dmart products, prices, offers, and you care about the customer like a close friend.

Customer name: ${customerName}
Customer preferences (what they usually buy): ${prefList}
Customer message: "${message}"
${dbInfo}

YOUR PERSONALITY:
- You are proactive, warm, like a knowledgeable friend who shops at Dmart regularly
- You subtly make customers feel that ordering from Zepto/Blinkit wastes money (delivery fees, surge pricing, convenience fees add up fast) without mentioning them by name
- Instead say things like "why pay extra for delivery when you can grab this fresh from Dmart yourself?" or "home delivery apps add 30-50% to your bill — picking it up yourself saves real money"
- You genuinely care — "this is what I'd tell my friend"
- For products NOT in database: use your knowledge of real Dmart India prices and availability
- Always be enthusiastic about offers and savings
- Reference their preferences naturally — "I know you love fruits, but since you asked about snacks..."

RESPOND WITH ONLY VALID JSON NO MARKDOWN:
{
  "intent": "browse_category|search_product|check_offers|check_list|question|greeting|out_of_scope",
  "search_keyword": "the main product/category to search in DB — single word or phrase",
  "product_list": [],
  "gemini_message": "your main reply message — warm, friendly, 2-3 sentences, mention savings vs delivery apps naturally when relevant",
  "follow_up_line": "one punchy line to end the conversation that makes them want to buy",
  "is_dmart_related": true or false,
  "online_products": []
}

If customer asks about a product NOT in the database, fill online_products with what you know:
"online_products": [{"name":"exact product name","category":"category","brand":"brand","price":actual_price_number,"available_at_dmart":true}]

Only add to online_products if you are confident this product is sold at Dmart India stores.
If not available at Dmart, set available_at_dmart: false.

check_list intent: when customer gives multiple items to check availability
product_list: extract items from check_list messages like "check milk, bread, lays"`;

  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g,'').trim();
    const parsed = JSON.parse(text);
    // Ensure arrays exist
    if (!parsed.product_list) parsed.product_list = [];
    if (!parsed.online_products) parsed.online_products = [];
    return parsed;
  } catch(e) {
    console.log('Gemini parse error:', e.message);
    return {
      intent:'search_product',
      search_keyword: message.split(' ')[0],
      product_list:[],
      gemini_message:`Sure ${customerName}! Let me find that for you right now!`,
      follow_up_line:'Come to Dmart and grab it fresh — no delivery fees!',
      is_dmart_related:true,
      online_products:[]
    };
  }
}

// ─────────────────────────────────────────
// SEND PRODUCT CARDS
// ─────────────────────────────────────────
async function sendProductCards(to, products, header) {
  await sendText(to, header);
  await sleep(800);
  for (let i=0; i<Math.min(products.length,5); i++) {
    const p = products[i];
    let cap = i===0 ? `⭐ *TOP PICK*\n` : '';
    cap += `*${p.name}*`;
    if (p.brand) cap += ` — ${p.brand}`;
    cap += `\n📦 ${p.category}\n`;
    if (p.discount_percent>0) {
      const save = (parseFloat(p.price)-parseFloat(p.final_price)).toFixed(0);
      cap += `~~Rs.${p.price}~~ → *Rs.${p.final_price}* 🏷️ *${p.discount_percent}% OFF*\n`;
      cap += `💰 You save Rs.${save} — that's real money back in your pocket!\n`;
      cap += `🔥 Offer ends soon — grab it before it's gone!`;
    } else {
      cap += `💵 *Rs.${p.price}*\n`;
      cap += `✅ Fresh stock available right now at Dmart!`;
    }
    cap += `\n\n📍 Pick it up yourself & save on delivery fees!\nReply *"order ${p.name}"* to note it!`;
    if (p.image_url && p.image_url.startsWith('http')) {
      await sendImage(to, p.image_url, cap);
    } else {
      await sendText(to, cap);
    }
    await sleep(700);
  }
}

// ─────────────────────────────────────────
// WELCOME MESSAGE
// ─────────────────────────────────────────
function buildWelcome(name) {
  return `👋 *Hey ${name}! Your Dmart friend is here!* 🛒

I'm your personal Dmart Assistant — think of me as that one friend who always knows what's on offer, what's fresh, and how to save the most money at Dmart!

Here's what I can do for you:

🎯 *Personalized Picks* — I know what you love, I'll find it instantly
🏷️ *Best Offers* — I'll show deals on things YOU actually buy
🛍️ *Any Product Search* — Snacks, dairy, fruits, beauty — just ask!
📋 *Shopping List Check* — Send your list, I'll check what's available and at what price
💡 *Smart Suggestions* — I'll tell you what's worth buying today vs skipping
🤝 *Honest Advice* — Like a friend, not a salesman

*Just chat naturally — I understand everything!*

Try: _"Show me snacks"_ or _"What offers today?"_ or _"Check: milk, bread, chips"_

What are you looking for today? 😊`;
}

// ─────────────────────────────────────────
// 1-MINUTE FOLLOW-UP
// ─────────────────────────────────────────
async function sendFollowUp(to, customer) {
  const prefs = await getPreferences(customer.customer_id);
  const prefNames = prefs.slice(0,3).map(p=>p.category).join(', ');
  const products = await getPreferredProducts(customer.customer_id, 4);
  const offers = await getOffers(customer.customer_id);
  const relevantOffers = offers.filter(o=>prefs.some(p=>p.category===o.category));

  await sendText(to,
    `🤔 *${customer.name}, still thinking?*\n\nYou know what — as your Dmart friend, I can't let you miss this 😄\n\nI know you always pick up *${prefNames}* from here. And right now? The offers on exactly those things are actually crazy good.\n\nLet me just show you — took me 2 seconds to find these for you! 👇`
  );
  await sleep(1500);

  if (products.length>0) {
    await sendProductCards(to, products,
      `💚 *Picked just for you ${customer.name} — from your favourite sections:*`
    );
  }

  if (relevantOffers.length>0) {
    await sleep(1000);
    await sendText(to, `🔥 *And THESE offers are live RIGHT NOW on what you buy regularly:*`);
    await sleep(600);
    for (let i=0; i<Math.min(relevantOffers.length,3); i++) {
      const o = relevantOffers[i];
      const cap = `🎉 *${o.name}*${o.brand?' — '+o.brand:''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n💰 Save Rs.${o.you_save} — just walk in and pick it up!\n\nHonestly? This is the kind of deal your friends would tell you about 😄\n\nReply *"order ${o.name}"* to note it!`;
      if (o.image_url && o.image_url.startsWith('http')) {
        await sendImage(to, o.image_url, cap);
      } else {
        await sendText(to, cap);
      }
      await sleep(700);
    }
  }

  await sendText(to,
    `😊 *${customer.name} — these are literally waiting for you at Dmart right now.*\n\nNo delivery wait. No surge fees. No "out of stock" surprises.\nJust fresh products at the real price — the way shopping should be! 🛒\n\nWhat do you want to grab today?`
  );
}

// ─────────────────────────────────────────
// SHOPPING LIST HANDLER
// ─────────────────────────────────────────
async function handleShoppingList(to, customer, productList, intro) {
  await sendText(to, `${intro}\n\n📋 *Checking your list right now ${customer.name}...*\nGive me a moment! ⚡`);
  await sleep(1000);

  const available = [];
  const addedFromOnline = [];
  const notAvailable = [];

  for (const item of productList) {
    const dbItem = await checkProductInDB(item);
    if (dbItem) { available.push({...dbItem, source:'db'}); continue; }

    // Ask Gemini about this specific product
    const gPrompt = `Is "${item}" sold at Dmart India stores? Reply ONLY valid JSON:
{"found":true/false,"name":"exact name","category":"category","brand":"brand or empty","price":price_as_number,"available_at_dmart":true/false}
Only mark found:true if genuinely sold at Dmart India. Price in Indian rupees.`;
    try {
      const gRes = await model.generateContent(gPrompt);
      const gText = gRes.response.text().trim().replace(/```json|```/g,'').trim();
      const gData = JSON.parse(gText);
      if (gData.found && gData.available_at_dmart) {
        await addProductToDB(gData.name, gData.category, gData.brand, gData.price);
        addedFromOnline.push(gData);
      } else {
        notAvailable.push(item);
      }
    } catch(e) { notAvailable.push(item); }
  }

  if (available.length>0) {
    await sendText(to, `✅ *Found in our Dmart store right now:*`);
    await sleep(400);
    for (const p of available) {
      let msg = `✅ *${p.name}*${p.brand?' — '+p.brand:''}\n`;
      msg += `📦 ${p.category} | 💵 Rs.${p.price}`;
      if (p.discount_percent>0) msg += `\n🏷️ *${p.discount_percent}% OFF → Rs.${p.final_price}* 🔥 Great timing!`;
      msg += `\n\n📍 In stock — walk in and grab it!\nReply *"order ${p.name}"* to note it.`;
      if (p.image_url && p.image_url.startsWith('http')) {
        await sendImage(to, p.image_url, msg);
      } else {
        await sendText(to, msg);
      }
      await sleep(600);
    }
  }

  if (addedFromOnline.length>0) {
    await sleep(500);
    await sendText(to, `🔍 *Also found at Dmart — just verified online:*`);
    await sleep(400);
    for (const p of addedFromOnline) {
      const msg = `🆕 *${p.name}*${p.brand?' — '+p.brand:''}\n📦 ${p.category}\n💵 Rs.${p.price} (approx Dmart price)\n✅ Available at Dmart!\n\n📍 Pick it up on your next visit!\nReply *"order ${p.name}"* to note it.`;
      await sendText(to, msg);
      await sleep(600);
    }
  }

  if (notAvailable.length>0) {
    await sleep(500);
    await sendText(to, `❌ *Not available at Dmart:*\n\n${notAvailable.map(n=>`• ${n}`).join('\n')}\n\nWant me to suggest similar alternatives we do have? Just say yes!`);
  }

  const total = available.length+addedFromOnline.length;
  await sleep(500);
  await sendText(to,
    `📊 *Your list summary ${customer.name}:*\n✅ Available at Dmart: *${total} items*\n❌ Not available: *${notAvailable.length} items*\n\n${total>0?`🛒 Everything available is fresh and ready — just walk in!\nNo delivery fees, no waiting. Real savings! 😊`:'Ask me to suggest alternatives for what\'s missing!'}`
  );
}

// ─────────────────────────────────────────
// MAIN PROCESS MESSAGE
// ─────────────────────────────────────────
async function processMessage(fromRaw, message) {
  console.log('MSG:', fromRaw, '|', message);
  const phone = fromRaw.replace('whatsapp:','').replace('+','');

  // Cancel pending follow-up — customer replied
  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
  }

  const customer = await getCustomerByPhone(phone);
  if (!customer) {
    await sendText(fromRaw, `👋 Welcome to *Dmart Assistant*!\n\nYour number isn't registered yet. Visit your nearest Dmart store to register and unlock personalized shopping!\n\nMeanwhile feel free to ask about any product. 🛒`);
    return;
  }

  const isNew = await isNewCustomer(customer.customer_id);
  const prefs = await getPreferences(customer.customer_id);

  // FIRST TIME — send welcome + schedule follow-up
  if (isNew) {
    const welcome = buildWelcome(customer.name);
    await sendText(fromRaw, welcome);
    await logInteraction(customer.customer_id, phone, 'first_time', 'welcome', null, welcome);
    const handle = setTimeout(async()=>{
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch(e){}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // Search DB first based on keywords before calling Gemini
  // Extract likely search keywords from message
  const msgWords = message.toLowerCase().replace(/[^a-z0-9 ]/g,'').split(' ')
    .filter(w=>w.length>2 && !['show','the','and','for','can','you','what','have','are','is','me','my','get','any'].includes(w));
  
  let dbResults = [];
  for (const word of msgWords.slice(0,3)) {
    const found = await searchProducts(word, customer.customer_id, 6);
    if (found.length>0) { dbResults = found; break; }
  }

  // Call Gemini with context
  const geminiData = await callGemini(message, customer.name, prefs, dbResults);
  const { intent, search_keyword, product_list, gemini_message, follow_up_line, is_dmart_related, online_products } = geminiData;

  // OUT OF SCOPE
  if (!is_dmart_related) {
    await sendText(fromRaw, `😄 ${gemini_message}\n\nI'm your *Dmart shopping buddy* — I'm only good at finding you amazing products and deals! 🛒\n\nAsk me about any product, offer, or category!`);
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, gemini_message);
    const handle = setTimeout(async()=>{
      try { await sendFollowUp(fromRaw, customer); pendingFollowUp.delete(phone); } catch(e){}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // SHOPPING LIST
  if (intent==='check_list' && product_list && product_list.length>0) {
    await handleShoppingList(fromRaw, customer, product_list, gemini_message);
    await logInteraction(customer.customer_id, phone, message, 'check_list', null, gemini_message);
    return;
  }

  // OFFERS
  if (intent==='check_offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length>0) {
      await sendText(fromRaw, `${gemini_message}\n\n🔥 *${offers.length} live deals right now — and many are on things YOU actually buy!*`);
      await sleep(800);
      for (let i=0; i<Math.min(offers.length,5); i++) {
        const o = offers[i];
        const cap = `🏷️ *${o.name}*${o.brand?' — '+o.brand:''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (*${o.discount_percent}% OFF*)\n💰 Save Rs.${o.you_save}!\n\n🤔 Think about it — that's real money saved just by walking into Dmart instead of ordering online.\n\nReply *"order ${o.name}"* to note it!`;
        if (o.image_url && o.image_url.startsWith('http')) {
          await sendImage(fromRaw, o.image_url, cap);
        } else {
          await sendText(fromRaw, cap);
        }
        await sleep(700);
      }
      await sendText(fromRaw, `💡 *${follow_up_line}*\n\nWant to browse a specific category or check your shopping list? Just tell me! 😊`);
    }
    await logInteraction(customer.customer_id, phone, message, 'check_offers', null, gemini_message);
    return;
  }

  // PRODUCT SEARCH / BROWSE CATEGORY
  // Use DB results first, fall back to keyword search, then online
  let products = dbResults;

  if (products.length===0 && search_keyword) {
    products = await searchProducts(search_keyword, customer.customer_id, 6);
  }

  // Add online products to DB if DB had nothing
  if (products.length===0 && online_products && online_products.length>0) {
    await sendText(fromRaw, `${gemini_message}\n\n🔍 *Checking Dmart catalog for you...*`);
    await sleep(800);
    const onlineAvailable = online_products.filter(p=>p.available_at_dmart);
    const notAtDmart = online_products.filter(p=>!p.available_at_dmart);

    if (onlineAvailable.length>0) {
      await sendText(fromRaw, `✅ *Found at Dmart India:*`);
      await sleep(400);
      for (const p of onlineAvailable) {
        await addProductToDB(p.name, p.category, p.brand, p.price);
        const msg = `🆕 *${p.name}*${p.brand?' — '+p.brand:''}\n📦 ${p.category}\n💵 Rs.${p.price}\n✅ Available at Dmart!\n\n📍 Fresh stock — pick it up yourself and skip the delivery fees!\nReply *"order ${p.name}"* to note it.`;
        await sendText(fromRaw, msg);
        await sleep(600);
      }
    }
    if (notAtDmart.length>0) {
      await sendText(fromRaw, `❌ *"${notAtDmart.map(p=>p.name).join(', ')}" is not available at Dmart.*\n\nWant me to suggest similar alternatives we do have? 😊`);
    }
    await sendText(fromRaw, `💡 *${follow_up_line}*`);
    await logInteraction(customer.customer_id, phone, message, intent, search_keyword, gemini_message);
    return;
  }

  if (products.length>0) {
    const header = `${gemini_message}\n\n🛒 *Found ${products.length} products for you ${customer.name}!*`;
    await sendProductCards(fromRaw, products, header);
    await sleep(500);

    // Check if any have offers
    const withOffers = products.filter(p=>p.discount_percent>0);
    if (withOffers.length>0) {
      await sendText(fromRaw, `🎉 *${withOffers.length} of these have active offers right now!*\n\n${follow_up_line}\n\nWant to see all current offers? Reply *"show offers"*! 🏷️`);
    } else {
      await sendText(fromRaw, `💡 *${follow_up_line}*\n\nReply *"show offers"* to see today's deals or ask for anything else! 😊`);
    }
  } else {
    // Nothing found anywhere
    await sendText(fromRaw,
      `${gemini_message}\n\n🤔 Hmm, I couldn't find that exact product right now.\n\nTry asking for:\n🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty | 🥤 Beverages\n\nOr send me your shopping list and I'll check everything! 📋`
    );
  }

  await logInteraction(customer.customer_id, phone, message, intent, search_keyword, gemini_message);
}

// ─────────────────────────────────────────
// ENDPOINTS
// ─────────────────────────────────────────
app.post('/whatsapp', (req, res) => {
  console.log('Incoming:', req.body.From, '|', req.body.Body);
  res.set('Content-Type','text/xml');
  res.send('<Response></Response>');
  const fromRaw = req.body.From||'';
  const message = req.body.Body||'';
  if (fromRaw && message) {
    processMessage(fromRaw, message).catch(e=>console.error('Error:',e.message));
  }
});

app.get('/health', (req,res) => res.json({ status:'ok', time:new Date().toISOString() }));

const PORT = process.env.PORT||3000;
app.listen(PORT, ()=>console.log(`Dmart AI running on port ${PORT}`));
