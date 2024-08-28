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
    res.send("Hello!");
  });

  // cost breakdown by worker
  app.get("/cost/worker", async (req, res) => {
    const workerIDs = req.query['id'];
    const totalCost = await totalCostByWorker(workerIDs);
    res.send(totalCost);
  });

  // cost breakdown by location
  app.get("/cost/location", async (req, res) => {
    const locationIDs = req.query['id'];
    const totalCost = await totalCostByLocation(locationIDs);
    res.send(totalCost);
  });

  app.listen(port, "0.0.0.0", () => {
    console.info(`App listening on ${port}.`);
  });
}

await main();

// Get a breakdown of total cost for all tasks at each location.
//
// Optional: Can filter by one or more location IDs
async function totalCostByLocation(locationIDs) {
  let query = `
    SELECT
      l.id,
      l.name as location_name,
      t.description as task_name,
      SUM(w.hourly_wage * (lt.time_seconds / 60)) as total_cost 
    FROM locations l
    JOIN tasks t
    ON t.location_id = l.id
    JOIN logged_time lt
    ON lt.task_id = t.id
    JOIN workers w
    ON w.id = lt.worker_id
  `
  if (locationIDs) {
    query += ` WHERE l.id IN (${locationIDs})`
  }

  return await doQuery(`${query} GROUP BY location_name, task_name;`);
}

// Get a breakdown of total cost for each worker across all tasks at each location.
//
// Optional: Can filter by one or more worker IDs
async function totalCostByWorker(workerIDs) {
  let query = `
    SELECT
      w.id,
      w.username,
      t.description as task,
      l.name as location,
      w.hourly_wage,
      SUM(lt.time_seconds) as total_time,
      SUM(w.hourly_wage * (lt.time_seconds / 60)) as total_cost 
    FROM workers w
    JOIN logged_time lt
    ON lt.worker_id = w.id 
    JOIN tasks t
    ON lt.task_id = t.id
    JOIN locations l
    ON t.location_id = l.id
  `
  if (workerIDs) {
    query += ` WHERE w.id IN (${workerIDs})`
  }

  return await doQuery(`${query} GROUP BY id, username, hourly_wage, description, name;`);
}

// Get a worker by ID
async function getWorkerByID(workerID) {
  const result = await doQuery(`SELECT * FROM workers WHERE id = ${workerID} LIMIT 1;`);
  return result[0] ?? null;
}

async function doQuery(query) {
  let conn;
  try {
    conn = await db.getConnection();
    const res = await conn.query(query);
    return res;
  } catch (err) {
    console.error(err);
  } finally {
    if (conn) conn.end();
  }
}