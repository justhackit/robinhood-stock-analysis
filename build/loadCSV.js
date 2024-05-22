"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const csv_parse_1 = require("csv-parse");
const pg_1 = require("pg");
// Function to read and parse the CSV file
function readCsvFile(filePath) {
    return new Promise((resolve, reject) => {
        const records = [];
        // Create a parser with options to handle new lines within quoted fields
        const parser = (0, csv_parse_1.parse)({
            delimiter: ',',
            quote: '"',
            relax_column_count: true,
            skip_empty_lines: true,
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
// Function to insert data into the PostgreSQL database
async function insertDataIntoDb(data, tableName) {
    const client = new pg_1.Client({
        user: 'your_username',
        host: 'your_host',
        database: 'your_database',
        password: 'your_password',
        port: 5432, // Default PostgreSQL port
    });
    await client.connect();
    try {
        for (const row of data) {
            const query = `INSERT INTO ${tableName} (column1, column2, column3) VALUES ($1, $2, $3)`;
            await client.query(query, row);
        }
        console.log('Data inserted successfully');
    }
    catch (error) {
        console.error('Error inserting data into DB:', error);
    }
    finally {
        await client.end();
    }
}
// Usage example
const filePath = '/Users/ajayed/Downloads/Robinhood_transactions_20200101_to_20240517.csv';
readCsvFile(filePath)
    .then((data) => {
    console.log('Data:', data);
    // insertDataIntoDb(data, 'transactions');
})
    .catch((error) => {
    console.error('Error:', error);
});
