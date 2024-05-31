/**
 * This is a trial module to query a CSV file as if it were a SQL database.
 * 
 * @module sql.js
 * @requires sql-csv
 */

const CsvSql = require("sql-csv");

/**
 * Create a new CsvSql instance.
 * @name csvSql
 * @type {Object}
 * @const
 * @instance
 * @memberof module:sql.js
 * @param {string} "ISDP LOGBOOK REPORT.csv" - The name of the CSV file to query.
 */
const csvSql = new CsvSql(["TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv"]);

/**
 * The main function of the module.
 * It queries the CSV file for all records and logs them to the console.
 * @async
 * @function main
 * @memberof module:sql.js
 * @example
 * main(); // Logs all records from the CSV file to the console.
 */
async function main() {
  /**
   * Query the CSV file for all records and log them to the console.
   * @name console.log
   * @function
   * @inner
   * @param {Promise<string>} csvSql.query('SELECT * FROM "ISDP LOGBOOK REPORT"') - The promise that resolves with all records from the CSV file.
   */
  console.log(await csvSql.query('SELECT * FROM "ISDP LOGBOOK REPORT"'));
}

main();