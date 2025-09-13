import express from "express";
import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();
const { Pool } = pkg;
const app = express();
app.use(express.json());

// Database connection
const pool = new Pool({
  user: process.env.DB_USER || "postgres",
  host: process.env.DB_HOST || "localhost",
  database: process.env.DB_NAME || "rfid_db",
  password: process.env.DB_PASS || "suthan2k21",
  port: process.env.DB_PORT || 5432,
});

// API: Get live zone occupancy
app.get("/api/zone-occupancy", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM rfid.v_zone_occupancy");
    res.json({
      success: true,
      data: result.rows,
    });
  } catch (err) {
    console.error("Error fetching occupancy:", err);
    res.status(500).json({ success: false, message: "Database error" });
  }
});

// Optional: trigger movement derivation manually
app.post("/api/derive-movements", async (req, res) => {
  try {
    await pool.query("SELECT rfid.refresh_and_load_movements()");
    res.json({ success: true, message: "Movements derived successfully" });
  } catch (err) {
    console.error("Error deriving movements:", err);
    res.status(500).json({ success: false, message: "Database error" });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
