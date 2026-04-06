// ============================================================
// DMART AI ASSISTANT — BACKEND API
// ============================================================
// HOW TO RUN:
//   npm init -y
//   npm install express pg @google/generative-ai cors dotenv
//   node STEP2_api_server.js
// ============================================================

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// ── DATABASE CONNECTION ──────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ── GEMINI AI SETUP ──────────────────────────────────────────
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });

// ============================================================
// CORE FUNCTION: Get customer by phone number
// WhatsApp sends phone number, we look up the customer
// ============================================================
async function getCustomerByPhone(phone) {
  // Normalize phone: remove +91, spaces, dashes
  const clean = phone.replace(/[^0-9]/g, '').replace(/^91/, '');
  const result = await pool.query(
    `SELECT * FROM customers WHERE phone_number LIKE $1 LIMIT 1`,
    [`%${clean}%`]
  );
  return result.rows[0] || null;
}

// ============================================================
// CORE FUNCTION: Smart Recommendation Engine
// Scores products based on:
//   - Customer's category preferences (weight: 60%)
//   - Active offers/discounts (weight: 30%)
//   - Purchase history if any (weight: 10%)
// ============================================================
async function getRecommendations(customerId, category = null, limit = 8) {
  const query = `
    SELECT 
      p.product_id,
      p.name,
      p.category,
      p.brand,
      p.price,
      p.image_url,
      p.stock_quantity,
      COALESCE(o.discount_percent, 0) as discount_percent,
      ROUND(p.price * (1 - COALESCE(o.discount_percent, 0) / 100.0), 2) as final_price,
      COALESCE(cp.preference_score, 0) as pref_score,
      COALESCE(cps.purchase_count, 0) as purchase_count,
      -- SCORING FORMULA: preference + offer boost + repeat boost
      (
        COALESCE(cp.preference_score, 0) * 6 +
        COALESCE(o.discount_percent, 0) * 3 +
        COALESCE(cps.purchase_count, 0) * 10
      ) as score
    FROM products p
    LEFT JOIN customer_preferences cp 
      ON cp.customer_id = $1 AND cp.category = p.category
    LEFT JOIN offers o 
      ON o.product_id = p.product_id AND o.valid_till > NOW()
    LEFT JOIN customer_product_stats cps 
      ON cps.customer_id = $1 AND cps.product_id = p.product_id
    WHERE p.is_available = true
      AND p.stock_quantity > 0
      ${category ? `AND p.category = $3` : ''}
    ORDER BY score DESC, o.discount_percent DESC NULLS LAST
    LIMIT $2
  `;

  const params = category
    ? [customerId, limit, category]
    : [customerId, limit];

  const result = await pool.query(query, params);
  return result.rows;
}

// ============================================================
// CORE FUNCTION: Get all active offers for a customer
// ============================================================
async function getOffersForCustomer(customerId) {
  const result = await pool.query(`
    SELECT 
      p.product_id,
      p.name,
      p.category,
      p.brand,
      p.price,
      o.discount_percent,
      ROUND(p.price * (1 - o.discount_percent / 100.0), 2) as offer_price,
      ROUND(p.price - (p.price * (1 - o.discount_percent / 100.0)), 2) as you_save,
      o.valid_till,
      COALESCE(cp.preference_score, 0) as relevance
    FROM offers o
    JOIN products p ON p.product_id = o.product_id
    LEFT JOIN customer_preferences cp 
      ON cp.customer_id = $1 AND cp.category = p.category
    WHERE o.valid_till > NOW()
      AND p.is_available = true
      AND p.stock_quantity > 0
    ORDER BY cp.preference_score DESC NULLS LAST, o.discount_percent DESC
    LIMIT 10
  `, [customerId]);
  return result.rows;
}

// ============================================================
// CORE FUNCTION: Detect intent using Gemini
// Returns: { intent, category, reply }
// ============================================================
async function detectIntentWithGemini(message, customerName, preferences) {
  const prefList = preferences.map(p => p.category).join(', ');

  const prompt = `
You are an AI assistant for a supermarket called Dmart. 
Customer name: ${customerName}
Their preferred categories: ${prefList || 'not known yet'}
Customer message: "${message}"

Analyze the message and respond with ONLY valid JSON (no markdown, no explanation):
{
  "intent": one of ["recommend", "browse_category", "check_offers", "place_order", "check_product", "greeting", "unknown"],
  "category": the product category mentioned or null,
  "extracted_products": list of specific product names mentioned or [],
  "confidence": number 0-1,
  "friendly_reply_prefix": a short warm reply in 1 sentence starting with the customer's name
}

Category options: Snacks, Dairy, Fruits, Vegetables, Instant Food, Beverages, Beauty, Personal Care, Household
`;

  try {
    const result = await model.generateContent(prompt);
    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    return JSON.parse(cleaned);
  } catch (e) {
    return {
      intent: 'recommend',
      category: null,
      extracted_products: [],
      confidence: 0.5,
      friendly_reply_prefix: `Hi ${customerName}!`
    };
  }
}

// ============================================================
// CORE FUNCTION: Build WhatsApp-friendly message
// ============================================================
function buildWhatsAppMessage(customerName, intent, products, offers, geminiPrefix) {
  let msg = `${geminiPrefix}\n\n`;

  if (intent === 'check_offers' && offers.length > 0) {
    msg += `*Today's deals just for you:*\n\n`;
    offers.slice(0, 6).forEach((o, i) => {
      msg += `${i + 1}. *${o.name}* (${o.brand || o.category})\n`;
      msg += `   ~~₹${o.price}~~ → *₹${o.offer_price}*  🏷️ ${o.discount_percent}% OFF\n`;
      msg += `   You save: ₹${o.you_save}\n\n`;
    });
    msg += `Reply with a product name to add it to your cart!`;
  } else if (products.length > 0) {
    msg += `*Recommended for you:*\n\n`;
    products.slice(0, 6).forEach((p, i) => {
      const hasOffer = p.discount_percent > 0;
      msg += `${i + 1}. *${p.name}*`;
      if (p.brand) msg += ` — ${p.brand}`;
      msg += `\n`;
      if (hasOffer) {
        msg += `   ~~₹${p.price}~~ → *₹${p.final_price}* (${p.discount_percent}% OFF)\n`;
      } else {
        msg += `   ₹${p.price}\n`;
      }
    });
    msg += `\nReply:\n• Product name to order\n• "offers" to see today's deals\n• Category name to browse more`;
  } else {
    msg += `I'll find the best products for you shortly. What category interests you?\n\nSnacks | Dairy | Fruits | Vegetables | Beverages`;
  }

  return msg;
}

// ============================================================
// CORE FUNCTION: Log interaction to database
// ============================================================
async function logInteraction(customerId, phone, message, intent, category, botResponse) {
  await pool.query(`
    INSERT INTO customer_interactions 
      (customer_id, phone_number, message, intent, category, bot_response)
    VALUES ($1, $2, $3, $4, $5, $6)
  `, [customerId, phone, message, intent, category, botResponse]);
}

// ============================================================
// CORE FUNCTION: Update purchase stats when order placed
// ============================================================
async function recordPurchase(customerId, productId) {
  await pool.query(`
    INSERT INTO customer_product_stats (customer_id, product_id, purchase_count, last_purchased)
    VALUES ($1, $2, 1, NOW())
    ON CONFLICT (customer_id, product_id)
    DO UPDATE SET 
      purchase_count = customer_product_stats.purchase_count + 1,
      last_purchased = NOW()
  `, [customerId, productId]);
}

// ============================================================
// API ENDPOINTS
// ============================================================

// ── MAIN ENDPOINT: n8n calls this with every WhatsApp message ──
// POST /chat
// Body: { phone: "9876543210", message: "show me snacks" }
app.post('/chat', async (req, res) => {
  const { phone, message } = req.body;

  if (!phone || !message) {
    return res.status(400).json({ error: 'phone and message required' });
  }

  try {
    // 1. Find customer by phone
    const customer = await getCustomerByPhone(phone);

    if (!customer) {
      return res.json({
        reply: `Welcome to Dmart! 👋\n\nI'm your personal shopping assistant. I don't have your account yet. Please register at our store or contact us to set up your profile.\n\nMeanwhile, I can show you our *best offers today*. Just reply "offers"!`,
        customer_found: false
      });
    }

    // 2. Get customer preferences
    const prefResult = await pool.query(
      `SELECT category, preference_score FROM customer_preferences WHERE customer_id = $1 ORDER BY preference_score DESC`,
      [customer.customer_id]
    );
    const preferences = prefResult.rows;

    // 3. Detect intent using Gemini
    const geminiResult = await detectIntentWithGemini(message, customer.name, preferences);
    const { intent, category, friendly_reply_prefix } = geminiResult;

    // 4. Fetch products/offers based on intent
    let products = [];
    let offers = [];

    if (intent === 'check_offers') {
      offers = await getOffersForCustomer(customer.customer_id);
      products = await getRecommendations(customer.customer_id, null, 4);
    } else if (intent === 'browse_category' && category) {
      products = await getRecommendations(customer.customer_id, category, 8);
    } else {
      // Default: personalized recommendations
      products = await getRecommendations(customer.customer_id, null, 8);
      // Always include offers if customer has matching categories
      offers = await getOffersForCustomer(customer.customer_id);
    }

    // 5. Build WhatsApp reply
    const reply = buildWhatsAppMessage(
      customer.name, intent, products, offers, friendly_reply_prefix
    );

    // 6. Log interaction
    await logInteraction(
      customer.customer_id, phone, message,
      intent, category, reply
    );

    res.json({
      reply,
      customer_id: customer.customer_id,
      customer_name: customer.name,
      intent,
      category,
      products_count: products.length,
      offers_count: offers.length
    });

  } catch (err) {
    console.error('Chat error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /recommendations?customer_id=1&limit=8 ──
app.get('/recommendations', async (req, res) => {
  const { customer_id, category, limit = 8 } = req.query;
  if (!customer_id) return res.status(400).json({ error: 'customer_id required' });

  try {
    const products = await getRecommendations(parseInt(customer_id), category || null, parseInt(limit));
    res.json({ customer_id, products, count: products.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /offers?customer_id=1 ──
app.get('/offers', async (req, res) => {
  const { customer_id } = req.query;
  if (!customer_id) return res.status(400).json({ error: 'customer_id required' });

  try {
    const offers = await getOffersForCustomer(parseInt(customer_id));
    res.json({ customer_id, offers, count: offers.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /order ── Records a purchase and updates behavior stats
app.post('/order', async (req, res) => {
  const { customer_id, product_ids } = req.body;
  if (!customer_id || !product_ids?.length) {
    return res.status(400).json({ error: 'customer_id and product_ids required' });
  }

  try {
    const orderResult = await pool.query(
      `INSERT INTO orders (customer_id, status) VALUES ($1, 'confirmed') RETURNING order_id`,
      [customer_id]
    );
    const orderId = orderResult.rows[0].order_id;

    for (const pid of product_ids) {
      const product = await pool.query(`SELECT price FROM products WHERE product_id = $1`, [pid]);
      if (product.rows[0]) {
        await pool.query(
          `INSERT INTO order_items (order_id, product_id, unit_price) VALUES ($1, $2, $3)`,
          [orderId, pid, product.rows[0].price]
        );
        await recordPurchase(customer_id, pid);
        // Update stock
        await pool.query(
          `UPDATE products SET stock_quantity = stock_quantity - 1 WHERE product_id = $1 AND stock_quantity > 0`,
          [pid]
        );
      }
    }

    res.json({ success: true, order_id: orderId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /customers — list all customers (for admin) ──
app.get('/customers', async (req, res) => {
  const result = await pool.query(`
    SELECT c.*, 
      COUNT(DISTINCT cp.category) as preference_count,
      COUNT(DISTINCT ci.id) as interaction_count
    FROM customers c
    LEFT JOIN customer_preferences cp ON cp.customer_id = c.customer_id
    LEFT JOIN customer_interactions ci ON ci.customer_id = c.customer_id
    GROUP BY c.customer_id
    ORDER BY c.customer_id
  `);
  res.json(result.rows);
});

// ── Health check ──
app.get('/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Dmart AI API running on http://localhost:${PORT}`);
  console.log(`Test: curl http://localhost:${PORT}/health`);
});
