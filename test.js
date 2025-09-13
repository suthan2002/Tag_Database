import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  user: "postgres",        // your DB user
  host: "localhost",       // your DB host
  database: "rfid_db",     // must match your database
  password: "suthan2k21",
  port: 5432,
});

pool.query("SELECT NOW()")
  .then(res => {
    console.log("DB Connected:", res.rows);
    pool.end();
  })
  .catch(err => {
    console.error("DB Connection Error:", err);
  });
