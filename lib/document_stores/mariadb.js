const mysql = require("mysql2");
const winston = require("winston");

var MySQLDocumentStore = function (options) {
  this.expire = options.expire;
  this.connectionConfig = {
    host: process.env.DB_HOST || options.host,
    user: process.env.DB_USER || options.user,
    port: process.env.DB_PORT || options.port || 3306,
    password: process.env.DB_PASSWORD || options.password,
    database: process.env.DB_NAME || options.database,
  };
};

/**
 * SET
 */
MySQLDocumentStore.prototype.set = function (key, data, callback, skipExpire) {
  const now = Math.floor(Date.now() / 1000);
  const expiration = this.expire && !skipExpire ? this.expire + now : -1;

  this.safeConnect((err, connection) => {
    if (err) return callback(false);

    const sql = `
            INSERT INTO entries (entry_id, value, expiration)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                expiration = 
                    CASE
                        WHEN expiration = -1 OR expiration > ?
                        THEN VALUES(expiration)
                        ELSE expiration
                    END
        `;

    connection.query(
      sql,
      [key, JSON.stringify(data), expiration, now],
      (err) => {
        connection.end();

        if (err) {
          winston.error("error persisting value to mysql", { error: err });
          return callback(false);
        }

        callback(true);
      }
    );
  });
};

/**
 * GET
 */
MySQLDocumentStore.prototype.get = function (key, callback, skipExpire) {
  const now = Math.floor(Date.now() / 1000);

  this.safeConnect((err, connection) => {
    if (err) return callback(false);

    const selectSql = `
            SELECT value, expiration
            FROM entries
            WHERE entry_id = ?
              AND (expiration = -1 OR expiration > ?)
            LIMIT 1
        `;

    connection.query(selectSql, [key, now], (err, rows) => {
      if (err) {
        connection.end();
        winston.error("error fetching value from mysql", { error: err });
        return callback(false);
      }

      if (!rows.length) {
        connection.end();
        return callback(false);
      }

      const entry = rows[0];
      const value = JSON.parse(entry.value);

      callback(value);

      if (entry.expiration !== -1 && this.expire && !skipExpire) {
        const updateSql = `
                    UPDATE entries
                    SET expiration = ?
                    WHERE entry_id = ?
                `;

        connection.query(updateSql, [this.expire + now, key], () =>
          connection.end()
        );
      } else {
        connection.end();
      }
    });
  });
};

/**
 * SAFE CONNECT
 */
MySQLDocumentStore.prototype.safeConnect = function (callback) {
  const connection = mysql.createConnection(this.connectionConfig);

  connection.connect((err) => {
    if (err) {
      winston.error("error connecting to mysql", { error: err });
      return callback(err);
    }

    const createTableSql = `
            CREATE TABLE IF NOT EXISTS entries (
                entry_id   VARCHAR(255) PRIMARY KEY,
                value      LONGTEXT NOT NULL,
                expiration INT NOT NULL,
                INDEX idx_expiration (expiration)
            )
        `;

    connection.query(createTableSql, (err) => {
      if (err) {
        winston.error("error creating mysql table", { error: err });
        connection.end();
        return callback(err);
      }

      callback(undefined, connection);
    });
  });
};

module.exports = MySQLDocumentStore;
