global.chai = require("chai");
global.assert = chai.assert;
global.expect = chai.expect;
chai.should();
chai.config.includeStack = true;

process.env.NODE_ENV = "test";
global.kuzu = process.env.TEST_INSTALLED
  ? require("kuzu")
  : require("../build/");

const tmp = require("tmp");
const fs = require("fs/promises");
const initTests = async () => {
  const dbPath = await new Promise((resolve, reject) => {
    tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
      if (err) {
        return reject(err);
      }
      return resolve(path);
    });
  });

  const db = new kuzu.Database(dbPath, 1 << 28 /* 256MB */);
  const conn = new kuzu.Connection(db, 4);

  const schema = (await fs.readFile("../../dataset/tinysnb/schema.cypher"))
    .toString()
    .split("\n");
  for (const line of schema) {
    if (line.trim().length === 0) {
      continue;
    }
    await conn.query(line);
  }

  const copy = (await fs.readFile("../../dataset/tinysnb/copy.cypher"))
    .toString()
    .split("\n");

  for (const line of copy) {
    if (line.trim().length === 0) {
      continue;
    }
    const statement = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
    await conn.query(statement);
  }

  global.dbPath = dbPath;
  global.db = db;
  global.conn = conn;
};

global.initTests = initTests;
