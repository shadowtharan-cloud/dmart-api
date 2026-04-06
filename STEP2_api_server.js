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

// ── PENDING REPLIES: track customers waiting for response ──
const pendingFollowUp = new Map(); // phone -> timeout handle

// ── SEND TEXT ──
async function sendText(to, body) {
  let toF = to;
  if (!toF.startsWith('whatsapp:')) {
    const d = toF.replace(/[^0-9]/g,'');
    toF = `whatsapp:+${d.startsWith('91')?d:'91'+d}`;
  }
  await twilioClient.messages.create({
    from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
    to: toF, body
  });
}

// ── SEND IMAGE ──
async function sendImage(to, imageUrl, caption) {
  let toF = to;
  if (!toF.startsWith('whatsapp:')) {
    const d = toF.replace(/[^0-9]/g,'');
    toF = `whatsapp:+${d.startsWith('91')?d:'91'+d}`;
  }
  try {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: toF, body: caption, mediaUrl: [imageUrl]
    });
  } catch(e) {
    await twilioClient.messages.create({
      from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
      to: toF, body: caption
    });
  }
}

// ── GET CUSTOMER ──
async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g,'').replace(/^91/,'');
  const r = await pool.query(`SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`,[`%${clean}%`]);
  return r.rows[0] || null;
}

// ── IS NEW CUSTOMER ──
async function isNewCustomer(customerId) {
  const r = await pool.query(`SELECT COUNT(*) as cnt FROM customer_interactions WHERE customer_id=$1`,[customerId]);
  return parseInt(r.rows[0].cnt) === 0;
}

// ── GET PREFERENCES ──
async function getPreferences(customerId) {
  const r = await pool.query(`SELECT category, preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`,[customerId]);
  return r.rows;
}

// ── GET RECOMMENDATIONS ──
async function getRecommendations(customerId, category, limit) {
  const q = `
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      (COALESCE(cp.preference_score,0)*6+COALESCE(o.discount_percent,0)*3+COALESCE(cps.purchase_count,0)*10) as score
    FROM products p
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    LEFT JOIN customer_product_stats cps ON cps.customer_id=$1 AND cps.product_id=p.product_id
    WHERE p.is_available=true AND p.stock_quantity>0
      ${category?'AND LOWER(p.category)=LOWER($3)':''}
    ORDER BY score DESC, RANDOM() LIMIT $2`;
  const r = await pool.query(q, category?[customerId,limit,category]:[customerId,limit]);
  return r.rows;
}

// ── GET PREFERRED PRODUCTS (products matching liked categories) ──
async function getPreferredProducts(customerId, limit) {
  const r = await pool.query(`
    SELECT p.product_id, p.name, p.category, p.brand, p.price, p.image_url,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      cp.preference_score,
      (cp.preference_score*6+COALESCE(o.discount_percent,0)*5) as score
    FROM products p
    JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE p.is_available=true AND p.stock_quantity>0
    ORDER BY score DESC LIMIT $2`,[customerId, limit]);
  return r.rows;
}

// ── GET OFFERS ──
async function getOffers(customerId) {
  const r = await pool.query(`
    SELECT p.name, p.brand, p.price, p.image_url, o.discount_percent,
      ROUND(p.price*(1-o.discount_percent/100.0),2) as offer_price,
      ROUND(p.price-(p.price*(1-o.discount_percent/100.0)),2) as you_save,
      COALESCE(cp.preference_score,0) as relevance
    FROM offers o JOIN products p ON p.product_id=o.product_id
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    WHERE o.valid_till>NOW() AND p.is_available=true AND p.stock_quantity>0
    ORDER BY relevance DESC, o.discount_percent DESC LIMIT 8`,[customerId]);
  return r.rows;
}

// ── CHECK PRODUCT IN DB ──
async function checkProductInDB(productName) {
  const r = await pool.query(`
    SELECT p.*, COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price
    FROM products p
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    WHERE LOWER(p.name) LIKE LOWER($1) AND p.is_available=true
    LIMIT 1`,[`%${productName}%`]);
  return r.rows[0] || null;
}

// ── ADD PRODUCT TO DB (from Gemini/internet knowledge) ──
async function addProductToDB(name, category, brand, price, imageUrl) {
  try {
    const r = await pool.query(`
      INSERT INTO products (name, category, brand, price, cost_price, stock_quantity, reorder_threshold, is_available, image_url)
      VALUES ($1,$2,$3,$4,$5,100,10,true,$6)
      ON CONFLICT DO NOTHING
      RETURNING product_id`,
      [name, category, brand||'', price, Math.round(price*0.7), imageUrl||'']);
    return r.rows[0]?.product_id || null;
  } catch(e) {
    console.log('Add product error:', e.message);
    return null;
  }
}

// ── LOG INTERACTION ──
async function logInteraction(customerId, phone, message, intent, category, reply) {
  try {
    await pool.query(`INSERT INTO customer_interactions(customer_id,phone_number,message,intent,category,bot_response) VALUES($1,$2,$3,$4,$5,$6)`,
      [customerId, phone, message, intent, category, reply]);
  } catch(e) {}
}

// ── SMART GEMINI: detect intent ──
async function detectIntent(message, customerName, prefs) {
  const prefList = prefs.map(p=>p.category).join(', ') || 'unknown';
  const prompt = `You are Dmart Assistant AI. Analyze this customer message.
Customer: ${customerName}
Preferences: ${prefList}
Message: "${message}"

Reply ONLY valid JSON no markdown:
{
  "intent": "recommend|browse_category|check_offers|check_list|question|greeting|out_of_scope",
  "category": null or "Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household",
  "product_list": [] or list of product names if customer gave a shopping list,
  "gemini_reply": "warm 1-2 sentence reply as a helpful friend who knows what they like",
  "is_dmart_related": true or false
}

check_list = when customer gives a list of items to check availability
out_of_scope = cricket, movies, politics, non-shopping topics`;

  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g,'').trim();
    return JSON.parse(text);
  } catch(e) {
    return { intent:'recommend', category:null, product_list:[], gemini_reply:`Sure ${customerName}, let me check that for you!`, is_dmart_related:true };
  }
}

// ── GEMINI: fetch product from internet knowledge ──
async function fetchProductFromGemini(productName) {
  const prompt = `You are a Dmart India supermarket product expert.
For the product "${productName}" sold at Dmart India stores, give me details.

Reply ONLY valid JSON no markdown:
{
  "found": true or false,
  "name": "exact product name as sold in Dmart",
  "category": "one of: Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household|Grains|Cleaning",
  "brand": "brand name or empty string",
  "price": approximate price in Indian rupees as a number,
  "available_at_dmart": true or false,
  "reason": "if not found, why"
}

Only return found:true if this product is actually sold at Dmart India stores.`;

  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g,'').trim();
    return JSON.parse(text);
  } catch(e) {
    return { found: false, reason: 'Could not fetch product info' };
  }
}

// ── SEND PRODUCTS WITH IMAGES ──
async function sendProducts(to, products, headerMsg) {
  await sendText(to, headerMsg);
  await new Promise(r=>setTimeout(r,800));
  for (let i=0; i<Math.min(products.length,4); i++) {
    const p = products[i];
    let cap = '';
    if (i===0) cap += `⭐ *TOP PICK*\n`;
    cap += `*${p.name}*${p.brand?' — '+p.brand:''}\n`;
    cap += `📦 ${p.category}\n`;
    if (p.discount_percent>0) {
      cap += `~~Rs.${p.price}~~ → *Rs.${p.final_price}* 🏷️ *${p.discount_percent}% OFF!*\n💰 You save Rs.${parseFloat(p.price)-parseFloat(p.final_price)}\n🔥 Limited offer!`;
    } else {
      cap += `💵 *Rs.${p.price}*`;
    }
    cap += `\n\nReply *"order ${p.name}"* to buy!`;
    if (p.image_url && p.image_url.startsWith('http')) {
      await sendImage(to, p.image_url, cap);
    } else {
      await sendText(to, cap);
    }
    await new Promise(r=>setTimeout(r,700));
  }
}

// ── WELCOME MESSAGE ──
function buildWelcome(name) {
  return `👋 *Hey ${name}! Welcome to Dmart Assistant!* 🛒

I'm your personal AI shopping buddy, built to make your Dmart experience amazing! Here's everything I can do for you:

🎯 *Smart Recommendations*
I know what you love to buy — I'll suggest products based on your taste, not random picks!
_Try: "Recommend something for me"_

🏷️ *Exclusive Offers & Deals*
I'll show you the best discounts on the products YOU like — real savings, not random offers!
_Try: "What offers today?"_

🛍️ *Browse Any Category*
Ask for any section of the store and I'll show you the best products with images!
_Try: "Show me snacks" or "Show dairy products"_

📋 *Shopping List Checker*
Give me your shopping list and I'll check what's available, the price, and if there are any offers!
_Try: "Check: milk, bread, lays chips, shampoo"_

❓ *Product Questions*
Ask anything about Dmart products — availability, price, alternatives!
_Try: "Is Amul butter available?" or "Best cooking oil?"_

🤝 *I'm Your Shopping Friend*
I know your preferences and I'll always find you the best deals on what you love!

Just chat naturally — I understand everything! 😊
*What would you like to explore today?*`;
}

// ── 1 MINUTE FOLLOW-UP MESSAGE ──
async function sendFollowUp(to, customer) {
  const prefs = await getPreferences(customer.customer_id);
  const prefCategories = prefs.slice(0,3).map(p=>p.category).join(', ');
  const products = await getPreferredProducts(customer.customer_id, 4);
  const offers = await getOffers(customer.customer_id);
  const offersOnLiked = offers.filter(o => prefs.some(p=>p.category===o.category));

  await sendText(to,
    `🤔 *${customer.name}, still thinking?*\n\nYou know what... I actually know what you love to buy at Dmart! 😏\n\nYou always pick up *${prefCategories}* — and guess what?\n\n🔥 *You won't believe the offers going on RIGHT NOW for exactly these products!*\n\nLet me show you — I think you'll love this! 👇`
  );
  await new Promise(r=>setTimeout(r,1500));

  if (products.length > 0) {
    await sendProducts(to, products,
      `💚 *These are from your favourite sections ${customer.name}!*\nI picked these just for you:`
    );
  }

  if (offersOnLiked.length > 0) {
    await new Promise(r=>setTimeout(r,1000));
    await sendText(to, `🏷️ *AND the offers on your favourites right now:*`);
    await new Promise(r=>setTimeout(r,500));
    for (let i=0; i<Math.min(offersOnLiked.length,3); i++) {
      const o = offersOnLiked[i];
      const cap = `🎉 *${o.name}*${o.brand?' — '+o.brand:''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}* (${o.discount_percent}% OFF)\n💰 Save Rs.${o.you_save} — just for you!\n\nReply *"order ${o.name}"* to grab it!`;
      if (o.image_url && o.image_url.startsWith('http')) {
        await sendImage(to, o.image_url, cap);
      } else {
        await sendText(to, cap);
      }
      await new Promise(r=>setTimeout(r,700));
    }
  }

  await sendText(to,
    `😊 *${customer.name}, these are literally made for you!*\n\nI'm your Dmart friend — I'll always show you the best stuff you love at the best price!\n\nJust reply and let's shop! 🛒`
  );
}

// ── HANDLE SHOPPING LIST ──
async function handleShoppingList(to, customer, productList, geminiReply) {
  await sendText(to, `${geminiReply}\n\n📋 *Checking your list right now ${customer.name}...*`);
  await new Promise(r=>setTimeout(r,1000));

  const available = [];
  const foundOnline = [];
  const notAvailable = [];

  for (const item of productList) {
    // Check in our database first
    const dbProduct = await checkProductInDB(item);
    if (dbProduct) {
      available.push({ ...dbProduct, source: 'db' });
      continue;
    }

    // Not in DB — ask Gemini/internet
    console.log('Fetching from Gemini:', item);
    const geminiProduct = await fetchProductFromGemini(item);

    if (geminiProduct.found && geminiProduct.available_at_dmart) {
      // Add to our database
      const newId = await addProductToDB(
        geminiProduct.name,
        geminiProduct.category,
        geminiProduct.brand,
        geminiProduct.price,
        ''
      );
      foundOnline.push({ ...geminiProduct, product_id: newId, source: 'gemini' });
    } else {
      notAvailable.push(item);
    }
  }

  // Show available products
  if (available.length > 0) {
    await sendText(to, `✅ *${available.length} item${available.length>1?'s':''} found in our store:*`);
    await new Promise(r=>setTimeout(r,500));
    for (const p of available) {
      let msg = `✅ *${p.name}*${p.brand?' — '+p.brand:''}\n📦 ${p.category} | 💵 Rs.${p.price}`;
      if (p.discount_percent > 0) msg += `\n🏷️ *${p.discount_percent}% OFF → Rs.${p.final_price}* 🔥`;
      msg += `\nReply *"order ${p.name}"* to add to cart!`;
      if (p.image_url && p.image_url.startsWith('http')) {
        await sendImage(to, p.image_url, msg);
      } else {
        await sendText(to, msg);
      }
      await new Promise(r=>setTimeout(r,700));
    }
  }

  // Show products found online and added to DB
  if (foundOnline.length > 0) {
    await new Promise(r=>setTimeout(r,500));
    await sendText(to, `🔍 *${foundOnline.length} more item${foundOnline.length>1?'s':''} found — just added to our catalog:*`);
    await new Promise(r=>setTimeout(r,500));
    for (const p of foundOnline) {
      const msg = `🆕 *${p.name}*${p.brand?' — '+p.brand:''}\n📦 ${p.category} | 💵 Rs.${p.price} (approx)\n✨ Just added to Dmart catalog!\n\nReply *"order ${p.name}"* to order!`;
      await sendText(to, msg);
      await new Promise(r=>setTimeout(r,700));
    }
  }

  // Show not available
  if (notAvailable.length > 0) {
    await new Promise(r=>setTimeout(r,500));
    const notAvailMsg = `❌ *Sorry, these are not available at Dmart:*\n\n${notAvailable.map(n=>`• ${n}`).join('\n')}\n\nWant me to suggest similar alternatives? Just ask!`;
    await sendText(to, notAvailMsg);
  }

  // Summary
  const total = available.length + foundOnline.length;
  await new Promise(r=>setTimeout(r,500));
  await sendText(to,
    `📊 *Shopping List Summary for ${customer.name}:*\n✅ Available: ${total} items\n❌ Not available: ${notAvailable.length} items\n\nWant to order anything? Just say *"order [product name]"*! 🛒`
  );
}

// ── MAIN PROCESS MESSAGE ──
async function processMessage(fromRaw, message) {
  console.log('MSG:', fromRaw, '|', message);
  const phone = fromRaw.replace('whatsapp:','').replace('+','');

  // Cancel any pending follow-up for this customer
  if (pendingFollowUp.has(phone)) {
    clearTimeout(pendingFollowUp.get(phone));
    pendingFollowUp.delete(phone);
  }

  const customer = await getCustomerByPhone(phone);

  if (!customer) {
    await sendText(fromRaw,
      `👋 Welcome to *Dmart Assistant*!\n\nYour number is not registered with us yet.\nPlease visit your nearest Dmart store to register and unlock personalized shopping!\n\nMeanwhile feel free to ask about our products. 🛒`
    );
    return;
  }

  const isNew = await isNewCustomer(customer.customer_id);
  const prefs = await getPreferences(customer.customer_id);

  // New customer — send welcome + schedule follow-up
  if (isNew) {
    const welcome = buildWelcome(customer.name);
    await sendText(fromRaw, welcome);
    await logInteraction(customer.customer_id, phone, message, 'welcome', null, welcome);

    // Schedule 1-minute follow-up if no reply
    const handle = setTimeout(async () => {
      try {
        await sendFollowUp(fromRaw, customer);
        pendingFollowUp.delete(phone);
      } catch(e) { console.error('Follow-up error:', e.message); }
    }, 60000); // 60 seconds
    pendingFollowUp.set(phone, handle);
    return;
  }

  // Detect intent
  const { intent, category, product_list, gemini_reply, is_dmart_related } = await detectIntent(message, customer.name, prefs);

  // Out of scope
  if (!is_dmart_related) {
    await sendText(fromRaw, `😄 ${gemini_reply}\n\nI'm your *Dmart shopping assistant* — I only help with shopping, products, and offers! 🛒\n\nAsk me anything about Dmart products!`);
    await logInteraction(customer.customer_id, phone, message, 'out_of_scope', null, gemini_reply);

    // Schedule follow-up if no activity
    const handle = setTimeout(async () => {
      try {
        await sendFollowUp(fromRaw, customer);
        pendingFollowUp.delete(phone);
      } catch(e) {}
    }, 60000);
    pendingFollowUp.set(phone, handle);
    return;
  }

  // Shopping list check
  if (intent === 'check_list' && product_list && product_list.length > 0) {
    await handleShoppingList(fromRaw, customer, product_list, gemini_reply);
    await logInteraction(customer.customer_id, phone, message, 'check_list', null, gemini_reply);
    return;
  }

  // Offers
  if (intent === 'check_offers') {
    const offers = await getOffers(customer.customer_id);
    if (offers.length > 0) {
      await sendText(fromRaw, `${gemini_reply}\n\n🔥 *${offers.length} deals found — and they're on stuff YOU love!*`);
      await new Promise(r=>setTimeout(r,800));
      for (let i=0; i<Math.min(offers.length,4); i++) {
        const o = offers[i];
        const cap = `🏷️ *${o.name}*${o.brand?' — '+o.brand:''}\n~~Rs.${o.price}~~ → *Rs.${o.offer_price}*\n💰 *${o.discount_percent}% OFF* — Save Rs.${o.you_save}\n🔥 Limited time!\n\nReply *"order ${o.name}"* to grab it!`;
        if (o.image_url && o.image_url.startsWith('http')) {
          await sendImage(fromRaw, o.image_url, cap);
        } else {
          await sendText(fromRaw, cap);
        }
        await new Promise(r=>setTimeout(r,700));
      }
      await sendText(fromRaw, `😊 *${customer.name} these offers are literally on your favourite things!*\n\nDon't miss them — reply *"order [product name]"* to buy!\n\nWant to see more? Just ask! 🛒`);
    }
    await logInteraction(customer.customer_id, phone, message, 'check_offers', null, gemini_reply);
    return;
  }

  // Browse category or recommend
  const products = intent === 'browse_category' && category
    ? await getRecommendations(customer.customer_id, category, 6)
    : await getPreferredProducts(customer.customer_id, 6);

  if (products.length > 0) {
    const header = intent === 'browse_category' && category
      ? `${gemini_reply}\n\n🛒 *Best ${category} products for you ${customer.name}!*`
      : `${gemini_reply}\n\n🎯 *${customer.name}, I picked these just for you — based on what I know you love!*`;
    await sendProducts(fromRaw, products, header);
    const offers = await getOffers(customer.customer_id);
    if (offers.length > 0) {
      await new Promise(r=>setTimeout(r,500));
      await sendText(fromRaw, `💡 *Psst ${customer.name}...* there are also *${offers.length} active offers* on products you like!\n\nReply *"show offers"* to see them! 🏷️`);
    }
  } else {
    await sendText(fromRaw, `${gemini_reply}\n\nHmm let me find something for you!\n\nTry: 🍎 Fruits | 🥛 Dairy | 🍟 Snacks | 🥦 Vegetables | 🧴 Beauty`);
  }

  await logInteraction(customer.customer_id, phone, message, intent, category, gemini_reply);
}

// ── WHATSAPP WEBHOOK ──
app.post('/whatsapp', (req, res) => {
  console.log('Incoming:', req.body.From, '|', req.body.Body);
  res.set('Content-Type','text/xml');
  res.send('<Response></Response>');
  const fromRaw = req.body.From || '';
  const message = req.body.Body || '';
  if (fromRaw && message) {
    processMessage(fromRaw, message).catch(e=>console.error('Error:',e.message));
  }
});

app.get('/health', (req,res) => res.json({ status:'ok', time:new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Dmart AI running on port ${PORT}`));
