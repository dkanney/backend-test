import express from "express";
import * as mariadb from "mariadb";

const app = express();
const port = 3000;
let db;

async function connect() {
  console.info("Connecting to DB...");
  db = mariadb.createPool({
    host: process.env["DATABASE_HOST"],
    user: process.env["DATABASE_USER"],
    password: process.env["DATABASE_PASSWORD"],
    database: process.env["DATABASE_NAME"]
  });

  const conn = await db.getConnection();
  try {
    await conn.query("SELECT 1");
  } finally {
    await conn.end();
  }
}

async function main() {
  await connect();

  app.get("/", (req, res) => {
    res.send("Hellos!");
  });

  app.get("/task", (req, res) => {
    res.send("Task!");
  });

  app.listen(port, "0.0.0.0", () => {
    console.info(`App listening on ${port}.`);
  });
}

await main();
