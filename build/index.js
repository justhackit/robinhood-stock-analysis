"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const csv_parse_1 = require("csv-parse");
const pg_1 = require("pg");
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
// Function to read and parse the CSV file
function readCsvFile(filePath) {
    //skip header
    return new Promise((resolve, reject) => {
        const records = [];
        // Create a parser with options to handle new lines within quoted fields
        const parser = (0, csv_parse_1.parse)({
            delimiter: ',',
            quote: '"',
            relax_column_count: true,
            skip_empty_lines: true,
            from_line: 2
        });
        // Read the file and pipe its content to the parser
        fs_1.default.createReadStream(filePath)
            .pipe(parser)
            .on('data', (row) => {
            records.push(row);
        })
            .on('end', () => {
            resolve(records);
        })
            .on('error', (error) => {
            reject(error);
        });
    });
}
function convertToTransaction(row) {
    // console.log(new Date(row[0]))
    return {
        activity_date: new Date(row[0]),
        process_date: new Date(row[1]),
        settle_date: new Date(row[2]),
        instrument: row[3],
        description: row[4].replace('\n', '|'),
        trans_code: row[5],
        quantity: parseFloat(row[6]),
        price: getNumberFromDollarAmount(row[7]),
        amount: getNumberFromDollarAmount(row[8]),
    };
}
function getNumberFromDollarAmount(amount) {
    let dollarAmount = amount.replace('$', '');
    if (dollarAmount.includes(',')) {
        dollarAmount = dollarAmount.replace(',', '');
    }
    if (dollarAmount.startsWith('(')) {
        dollarAmount = dollarAmount.replace('(', '-').replace(')', '');
    }
    return parseFloat(dollarAmount);
}
// Function to insert data into the PostgreSQL database
async function insertDataIntoDb(data, tableName) {
    const client = new pg_1.Client({
        user: process.env.POSTGRES_USERNAME,
        host: process.env.POSTGRES_HOST,
        database: process.env.POSTGRES_DATABASE,
        password: process.env.POSTGRES_PASSWORD,
        port: process.env.POSTGRES_PORT,
    });
    console.log('Connecting to DB...');
    await client.connect();
    let tracking = 0;
    try {
        for (const row of data) {
            const query = `INSERT INTO ${tableName}
                (activity_date,process_date,settle_date,instrument,description,trans_code,quantity,price,amount) VALUES ($1, $2, $3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (activity_date, instrument, description, trans_code, quantity, price, amount) DO NOTHING`;
            await client.query(query, [
                row.activity_date,
                row.process_date,
                row.settle_date,
                row.instrument,
                row.description,
                row.trans_code,
                row.quantity,
                row.price,
                row.amount,
            ]);
            tracking++;
        }
        console.log('Data inserted successfully');
    }
    catch (error) {
        console.error(`Error inserting ${JSON.stringify(data[tracking])} into DB:`, error);
    }
    finally {
        console.log('Closing DB connection...');
        await client.end();
    }
}
// Usage exampl
// const filePath = '/Users/ajayed/Downloads/Robinhood_transactions_20200101_to_20240517.csv';
const filePath = './data/Robinhood_transactions_20180319_to_20191231.csv';
console.log('Current working directory:', process.cwd());
readCsvFile(filePath)
    .then((data) => {
    //convert data into array of Transaction
    const modeled = data.map(row => convertToTransaction(row));
    //print first 10 rows of
    // console.log(modeled.slice(0, 2));
    insertDataIntoDb(modeled, 'stocks.robinhood_raw_transactions');
})
    .catch((error) => {
    console.error('Error: ', error);
});
