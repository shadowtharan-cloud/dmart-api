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

async function sendWhatsAppReply(toPhone, message) {
  let to = toPhone;
  if (!to.startsWith('whatsapp:')) {
    const d = to.replace(/[^0-9]/g, '');
    to = `whatsapp:+${d.startsWith('91') ? d : '91' + d}`;
  }
  await twilioClient.messages.create({
    from: `whatsapp:${process.env.TWILIO_WHATSAPP_NUMBER}`,
    to: to,
    body: message
  });
  console.log('Sent to', to);
}

async function getCustomerByPhone(phone) {
  const clean = phone.replace(/[^0-9]/g, '').replace(/^91/, '');
  const r = await pool.query(`SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`, [`%${clean}%`]);
  return r.rows[0] || null;
}

async function getRecommendations(customerId, category, limit) {
  const q = `
    SELECT p.product_id, p.name, p.category, p.brand, p.price,
      COALESCE(o.discount_percent,0) as discount_percent,
      ROUND(p.price*(1-COALESCE(o.discount_percent,0)/100.0),2) as final_price,
      (COALESCE(cp.preference_score,0)*6+COALESCE(o.discount_percent,0)*3+COALESCE(cps.purchase_count,0)*10) as score
    FROM products p
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    LEFT JOIN offers o ON o.product_id=p.product_id AND o.valid_till>NOW()
    LEFT JOIN customer_product_stats cps ON cps.customer_id=$1 AND cps.product_id=p.product_id
    WHERE p.is_available=true AND p.stock_quantity>0 ${category?'AND p.category=$3':''}
    ORDER BY score DESC LIMIT $2`;
  const r = await pool.query(q, category?[customerId,limit,category]:[customerId,limit]);
  return r.rows;
}

async function getOffers(customerId) {
  const r = await pool.query(`
    SELECT p.name, p.brand, p.price, o.discount_percent,
      ROUND(p.price*(1-o.discount_percent/100.0),2) as offer_price,
      ROUND(p.price-(p.price*(1-o.discount_percent/100.0)),2) as you_save,
      COALESCE(cp.preference_score,0) as relevance
    FROM offers o JOIN products p ON p.product_id=o.product_id
    LEFT JOIN customer_preferences cp ON cp.customer_id=$1 AND cp.category=p.category
    WHERE o.valid_till>NOW() AND p.is_available=true AND p.stock_quantity>0
    ORDER BY cp.preference_score DESC NULLS LAST, o.discount_percent DESC LIMIT 10`, [customerId]);
  return r.rows;
}

async function detectIntent(message, name, prefs) {
  const prompt = `You are Dmart AI assistant.
Customer: ${name}
Likes: ${prefs.map(p=>p.category).join(', ')||'unknown'}
Message: "${message}"
Reply ONLY with JSON, no markdown:
{"intent":"recommend","category":null,"prefix":"Hi ${name}!"}
intent = recommend|browse_category|check_offers|greeting|unknown
category = Snacks|Dairy|Fruits|Vegetables|Instant Food|Beverages|Beauty|Personal Care|Household|null`;
  try {
    const res = await model.generateContent(prompt);
    const text = res.response.text().trim().replace(/```json|```/g,'').trim();
    return JSON.parse(text);
  } catch(e) {
    return { intent:'recommend', category:null, prefix:`Hi ${name}!` };
  }
}

function buildReply(intent, products, offers, prefix) {
  let msg = `${prefix}\n\n`;
  if (intent==='check_offers' && offers.length>0) {
    msg += `*Today\'s deals for you:*\n\n`;
    offers.slice(0,6).forEach((o,i) => {
      msg += `${i+1}. *${o.name}*\n   Rs.${o.price} -> *Rs.${o.offer_price}* (${o.discount_percent}% OFF)\n   Save: Rs.${o.you_save}\n\n`;
    });
    msg += `Reply with product name to order!`;
  } else if (products.length>0) {
    msg += `*Recommended for you:*\n\n`;
    products.slice(0,6).forEach((p,i) => {
      msg += `${i+1}. *${p.name}*${p.brand?' - '+p.brand:''}\n`;
      msg += p.discount_percent>0 ? `   Rs.${p.price} -> *Rs.${p.final_price}* (${p.discount_percent}% OFF)\n` : `   Rs.${p.price}\n`;
    });
    msg += `\nReply:\n- Product name to order\n- "offers" for deals\n- Category name to browse`;
  } else {
    msg += `What are you looking for?\nSnacks | Dairy | Fruits | Vegetables | Beverages`;
  }
  return msg;
}

async function processMessage(fromRaw, message) {
  console.log('Processing:', fromRaw, message);
  const phone = fromRaw.replace('whatsapp:','').replace('+','');
  const customer = await getCustomerByPhone(phone);

  if (!customer) {
    await sendWhatsAppReply(fromRaw, `Welcome to Dmart! Your number is not registered yet.\n\nReply "offers" to see today\'s best deals!`);
    return;
  }

  const prefs = (await pool.query(`SELECT category,preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`,[customer.customer_id])).rows;
  const { intent, category, prefix } = await detectIntent(message, customer.name, prefs);

  let products=[], offers=[];
  if (intent==='check_offers') {
    offers = await getOffers(customer.customer_id);
    products = await getRecommendations(customer.customer_id, null, 4);
  } else if (intent==='browse_category' && category) {
    products = await getRecommendations(customer.customer_id, category, 8);
  } else {
    products = await getRecommendations(customer.customer_id, null, 8);
    offers = await getOffers(customer.customer_id);
  }

  const reply = buildReply(intent, products, offers, prefix);

  try {
    await pool.query(`INSERT INTO customer_interactions(customer_id,phone_number,message,intent,category,bot_response) VALUES($1,$2,$3,$4,$5,$6)`,
      [customer.customer_id, phone, message, intent, category, reply]);
  } catch(e) {}

  await sendWhatsAppReply(fromRaw, reply);
  console.log('Done for', customer.name);
}

// ── MAIN WHATSAPP ENDPOINT ──
// Responds to Twilio INSTANTLY then processes in background
// This prevents Twilio 15-second timeout
app.post('/whatsapp', (req, res) => {
  console.log('Incoming WhatsApp:', req.body.From, req.body.Body);

  // Respond to Twilio immediately — under 1 second
  res.set('Content-Type', 'text/xml');
  res.send('<Response></Response>');

  // Process message in background — no timeout possible
  const fromRaw = req.body.From || '';
  const message = req.body.Body || '';
  if (fromRaw && message) {
    processMessage(fromRaw, message).catch(err => {
      console.error('Process error:', err.message);
    });
  }
});

app.post('/chat', async (req, res) => {
  const { phone, message } = req.body;
  if (!phone || !message) return res.status(400).json({ error: 'phone and message required' });
  try {
    const clean = phone.replace(/[^0-9]/g,'').replace(/^91/,'');
    const customer = await getCustomerByPhone(clean);
    const prefs = customer ? (await pool.query(`SELECT category,preference_score FROM customer_preferences WHERE customer_id=$1 ORDER BY preference_score DESC`,[customer.customer_id])).rows : [];
    const { intent, category, prefix } = await detectIntent(message, customer?.name||'Customer', prefs);
    let products=[], offers=[];
    if (intent==='check_offers') { offers=await getOffers(customer?.customer_id); }
    else if (intent==='browse_category'&&category) { products=await getRecommendations(customer?.customer_id,category,8); }
    else { products=await getRecommendations(customer?.customer_id,null,8); offers=await getOffers(customer?.customer_id); }
    const reply = buildReply(intent, products, offers, prefix);
    res.json({ reply, customer_name: customer?.name, intent });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/recommendations', async (req,res) => {
  const { customer_id, category, limit=8 } = req.query;
  if (!customer_id) return res.status(400).json({ error: 'customer_id required' });
  try {
    const p = await getRecommendations(parseInt(customer_id), category||null, parseInt(limit));
    res.json({ customer_id, products:p, count:p.length });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/health', (req,res) => res.json({ status:'ok', time:new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Dmart AI API running on port ${PORT}`);
  console.log(`WhatsApp endpoint: /whatsapp`);
});
