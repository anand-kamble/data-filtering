const CsvSql = require("sql-csv");

const csvSql = new CsvSql("ISDP LOGBOOK REPORT.csv");

async function main() {
  console.log(await csvSql.query('SELECT * FROM "ISDP LOGBOOK REPORT"'));
}

main();