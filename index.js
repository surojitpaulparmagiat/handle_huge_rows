const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const {Readable} = require('stream');
const {DateTime} = require("luxon");
const Joi = require('joi');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const mysql = require('mysql2/promise');

// MySQL Database Configuration
const DB_CONFIG = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'password',
    database: '1audittest'
};


const extractNegativeAuditNumber = (number) => {
    // Handle null/undefined early
    if (number == null || number === '') return 0;

    // Trim whitespace from the input number
    const str = number.toString().trim();

    // Check if the number is in parentheses, indicating a negative value
    if (str.startsWith('(') && str.endsWith(')')) {
        // Extract the value inside parentheses and make it negative
        return -parseFloat(str.slice(1, -1).replace(/,/g, ''));
    }

    // Remove commas and parse
    return parseFloat(str.replace(/,/g, ''));
}


// ========== POSITION-BASED MAPPING (Array - FASTER!) ==========
// Map by column position using ARRAY for direct O(1) access
// Index = column position, Value = your custom key name
const positionMappingArray = [
    'description',        // Position 0: First column (البيان)
    '',   // Position 1: Second column (رقم القيد)
    'issue_date',         // Position 2: Third column (Issue date)
    'transaction_number', // Position 3: Fourth column
    'account_name',       // Position 4: Fifth column
    'tb_account_code',    // Position 5: Sixth column
    'amount_debit',       // Position 6: Seventh column
    'amount_credit',      // Position 7: Eighth column
    'account_number'      // Position 8: Ninth column
];

const amount_debit_credit_same_column = false;

const zeroIfEmpty = (value) => {
    // Handle null, undefined, empty string, or NaN
    if (value == null || value === '' || Number.isNaN(value)) {
        return 0;
    }
    return Number(value);
};

const numberParseFunction = (raw_value) => zeroIfEmpty(extractNegativeAuditNumber(raw_value));

const issue_date_date_format = 'dd-MM-yyyy';

const dateFormatDateISO = (date, df = 'dd/MM/yyyy') => {
    if (!date) {
        return "";
    }

    // Try as JS Date first
    const js_date = DateTime.fromJSDate(date);
    if (js_date.isValid) {
        return js_date.toFormat(df);
    }

    // Try as ISO string
    const iso_date = DateTime.fromISO(date);
    if (iso_date.isValid) {
        return iso_date.toFormat(df);
    }

    return "";
};

const dateToDBFormat = (date) => {
    return dateFormatDateISO(date, "yyyy-MM-dd");
}

const dateTryParseFromFormat = (date_string, format_string) => {
    const luxon_date = DateTime.fromFormat(date_string, format_string);
    return luxon_date.isValid ? dateToDBFormat(luxon_date) : null;
}


// validate the mapped row.


// Function to transform row data by column position using ARRAY
function transformRowByPosition(row, posMappingArray = positionMappingArray) {
    const values = Object.values(row);

    // note: convert 'A' to 0 position, 'B' to 1 position, etc. make empty as '' string and handle.
    const transformedRow = {
        // Initialize all expected keys to null
        description: null,
        reference_number: null,
        issue_date: null,
        transaction_number: null,
        account_name: null,
        account_number: null,
        amount_debit: null,
        amount_credit: null,
        amount: null,
        trial_balance_account_id: null,
        transaction_type: null,
        transaction_created_by: null,
    };

    // Map values according to current column positions
    for (let i = 0; i < values.length; i++) {
        const mappingKey = posMappingArray[i];

        // If mapping key is an empty string or undefined/null, skip this column entirely
        if (mappingKey === '') {
            continue;
        }

        const key = mappingKey || `col_${i}`;
        transformedRow[key] = values[i];
    }

    // Compute amount from debit/credit fields
    const amount_debit_raw = transformedRow.amount_debit;
    const amount_credit_raw = transformedRow.amount_credit;

    let amount;
    if (amount_debit_credit_same_column) {
        amount = numberParseFunction(amount_debit_raw);
    } else {
        const amount_debit = numberParseFunction(amount_debit_raw);
        const amount_credit = numberParseFunction(amount_credit_raw);
        amount = Math.abs(amount_debit) - Math.abs(amount_credit);
    }

    transformedRow.amount = amount;
    transformedRow.trial_balance_account_id = null;

    // Normalize date
    const issue_date_raw = transformedRow.issue_date;
    transformedRow.issue_date = issue_date_raw
        ? (dateTryParseFromFormat(issue_date_raw, issue_date_date_format) || '')
        : '';

    // Remove intermediate debit/credit if you don't want them in final row
    delete transformedRow.amount_debit;
    delete transformedRow.amount_credit;

    return transformedRow;
}

const maxRows = 10_000;
const ROWS_PER_FILE = 5_000; // Create new CSV file every 20K rows
const TEMP_FOLDER = './temp';


// Ensure temp folder exists
if (!fs.existsSync(TEMP_FOLDER)) {
    fs.mkdirSync(TEMP_FOLDER, {recursive: true});
}

// Helper to convert JS values to MySQL LOAD DATA representation
// We use \\N (unquoted) so MySQL interprets it as NULL
function toMysqlCsvValue(value) {
    if (value === null || value === undefined || value === '') {
        return '\\N';
    }
    return value;
}

// Function to write batch to CSV file
async function writeBatchToCSV(rows, batchNumber) {
    if (rows.length === 0) return;

    const fileName = `CSV_${batchNumber}.csv`;
    const filePath = path.join(TEMP_FOLDER, fileName);

    const csvWriter = createCsvWriter({
        path: filePath,
        header: [
            {id: 'description', title: 'description'},
            {id: 'reference_number', title: 'reference_number'},
            {id: 'issue_date', title: 'issue_date'},
            {id: 'transaction_number', title: 'transaction_number'},
            {id: 'account_name', title: 'account_name'},
            {id: 'account_number', title: 'account_number'},
            {id: 'amount', title: 'amount'},
            {id: 'trial_balance_account_id', title: 'trial_balance_account_id'},
            {id: 'transaction_type', title: 'transaction_type'},
            {id: 'transaction_created_by', title: 'transaction_created_by'},
        ]
    });

    // Map rows to ensure NULL-like values become \N for MySQL
    const safeRows = rows.map(r => ({
        description: toMysqlCsvValue(r.description),
        reference_number: toMysqlCsvValue(r.reference_number),
        issue_date: toMysqlCsvValue(r.issue_date),
        transaction_number: toMysqlCsvValue(r.transaction_number),
        account_name: toMysqlCsvValue(r.account_name),
        account_number: toMysqlCsvValue(r.account_number),
        amount: toMysqlCsvValue(r.amount),
        trial_balance_account_id: toMysqlCsvValue(r.trial_balance_account_id),
        transaction_type: toMysqlCsvValue(r.transaction_type),
        transaction_created_by: toMysqlCsvValue(r.transaction_created_by),
    }));

    await csvWriter.writeRecords(safeRows);
    return filePath;
}

// Function to write invalid rows to CSV file
async function writeInvalidRowsToCSV(invalidRows) {
    if (invalidRows.length === 0) return null;

    const fileName = 'INVALID_ROWS.csv';
    const filePath = path.join(TEMP_FOLDER, fileName);

    const csvWriter = createCsvWriter({
        path: filePath,
        header: [
            {id: 'row_number', title: 'row_number'},
            {id: 'error_reason', title: 'error_reason'},
        ]
    });

    // Format invalid rows for CSV
    const formattedRows = invalidRows.map(item => ({
        row_number: item.rowNumber,
        error_reason: item.error,
    }));

    await csvWriter.writeRecords(formattedRows);
    console.log(`\n📄 Created ${fileName} with ${invalidRows.length} invalid rows`);
    return filePath;
}

// Function to load CSV files into MySQL database
async function loadCSVFilesToMySQL(csvFiles) {
    if (!csvFiles || csvFiles.length === 0) {
        console.log('No CSV files to load into database');
        return;
    }

    let connection;
    try {
        console.log('\n🔄 Connecting to MySQL database...');
        connection = await mysql.createConnection({
            ...DB_CONFIG,
            infileStreamFactory: (path) => fs.createReadStream(path),
        });

        // Disable constraints for faster bulk insert
        await connection.query('SET foreign_key_checks = 0');
        await connection.query('SET unique_checks = 0');
        await connection.query('SET autocommit = 0');

        let totalRowsInserted = 0;

        for (let i = 0; i < csvFiles.length; i++) {
            const filePath = csvFiles[i];
            const absolutePath = path.resolve(filePath).replace(/\\/g, '/');

            console.log(`\n📥 Loading ${path.basename(filePath)} into database...`);

            const loadDataSQL = `
                LOAD DATA LOCAL INFILE '${absolutePath}'
                INTO TABLE aud_source_data
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (description, reference_number, issue_date, transaction_number,
                 account_name, account_number, amount, trial_balance_account_id,
                 transaction_type, transaction_created_by)
                SET source_data_import_session_id = 1, createdAt = NOW(), updatedAt = NOW()
            `;

            try {
                const [result] = await connection.query(loadDataSQL);
                const rowsAffected = result.affectedRows || 0;
                totalRowsInserted += rowsAffected;
                console.log(`✅ Loaded ${rowsAffected.toLocaleString()} rows from ${path.basename(filePath)}`);
            } catch (err) {
                console.error(`❌ Error loading ${path.basename(filePath)}:`, err.message);
            }
        }

        await connection.query('COMMIT');
        await connection.query('SET foreign_key_checks = 1');
        await connection.query('SET unique_checks = 1');
        await connection.query('SET autocommit = 1');

        console.log(`\n${'='.repeat(60)}`);
        console.log(`✅ Database import completed!`);
        console.log(`Total rows inserted: ${totalRowsInserted.toLocaleString()}`);
        console.log(`${'='.repeat(60)}`);

        return totalRowsInserted;

    } catch (error) {
        console.error('❌ Database error:', error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
            console.log('\n🔌 Database connection closed');
        }
    }
}


async function readCSVFileWithStreaming(source, useMapping = true) {
    return new Promise((resolve, reject) => {
        let stream;

        // Check if source is a Buffer (from S3) or a file path
        if (Buffer.isBuffer(source)) {
            stream = Readable.from(source);
        } else if (typeof source === 'string') {
            stream = fs.createReadStream(source);
        } else if (source.readable) {
            stream = source;
        } else {
            return reject(new Error('Invalid source: must be a file path, Buffer, or readable stream'));
        }

        // Local arrays for this processing session
        const mapped_rows = [];
        const mapped_invalid_rows = [];

        let rowCount = 0;
        let batchNumber = 1;
        let filesCreated = [];
        let lastBatchWrittenAt = 0; // Track how many valid rows were written

        stream
            .pipe(csv())
            .on('data', (row) => {
                if (rowCount < maxRows) {
                    rowCount++;

                    // Apply position-based mapping if enabled
                    const outputRow = useMapping
                        ? transformRowByPosition(row, positionMappingArray)
                        : row;

                    // Validate the row
                    const {error} = SamplingSourceDataValidationSchema.importValidation.validate(outputRow, {
                        allowUnknown: true,
                        abortEarly: false,
                        errors: {
                            escapeHtml: false,
                            wrap: {
                                label: '',
                            },
                        },
                    });

                    if (error) {
                        mapped_invalid_rows.push({
                            rowNumber: rowCount,
                            row: outputRow,
                            error: error.details.map(d => d.message).join(', '),
                        });
                    } else {
                        // Only add valid rows
                        mapped_rows.push(outputRow);
                    }

                    // Check if we need to write current batch to file
                    const validRowsSinceLastWrite = mapped_rows.length - lastBatchWrittenAt;

                    if (validRowsSinceLastWrite >= ROWS_PER_FILE) {
                        // Capture current batch info before updating counters
                        const currentBatchNumber = batchNumber;
                        const batchStart = lastBatchWrittenAt;
                        const batchEnd = mapped_rows.length;

                        // Update counters immediately to prevent duplicate detection
                        lastBatchWrittenAt = batchEnd;
                        batchNumber++;

                        // Write asynchronously without blocking - different files can write concurrently!
                        const batchToWrite = mapped_rows.slice(batchStart, batchEnd);
                        writeBatchToCSV(batchToWrite, currentBatchNumber)
                            .then(filePath => {
                                filesCreated.push(filePath);
                                console.log(`✅ Created CSV_${currentBatchNumber}.csv with ${batchToWrite.length} rows`);
                            })
                            .catch(err => {
                                console.error(`❌ Error writing batch ${currentBatchNumber}:`, err);
                            });
                    }
                }

                if (rowCount >= maxRows) {
                    stream.destroy();
                }
            })
            .on('end', () => {
                // This runs when stream completes naturally (not destroyed)
                printSummary();
            })
            .on('error', (error) => {
                reject(error);
            });

        stream.on('close', () => {
            // This runs when stream is destroyed or closed
            printSummary();
        });

        const printSummary = async () => {
            // Prevent printing summary twice
            if (printSummary.printed) return;
            printSummary.printed = true;

            // Wait a moment for async file writes to complete
            await new Promise(resolve => setTimeout(resolve, 200));

            // Write any remaining rows that haven't been written yet
            const remainingRows = mapped_rows.length - lastBatchWrittenAt;
            if (remainingRows > 0) {
                console.log(`\n📝 Writing final batch with ${remainingRows} remaining rows...`);
                const filePath = await writeBatchToCSV(
                    mapped_rows.slice(lastBatchWrittenAt),
                    batchNumber
                );
                filesCreated.push(filePath);
                console.log(`✅ Created CSV_${batchNumber}.csv with ${remainingRows} rows`);
            }

            // Write invalid rows to CSV if any exist
            if (mapped_invalid_rows.length > 0) {
                await writeInvalidRowsToCSV(mapped_invalid_rows);
            }

            // Print summary
            console.log(`\n${'='.repeat(60)}`);
            console.log(`Total rows processed: ${rowCount.toLocaleString()}`);
            console.log(`Valid rows: ${mapped_rows.length.toLocaleString()}`);
            console.log(`Invalid rows: ${mapped_invalid_rows.length.toLocaleString()}`);
            console.log(`Missing rows: ${(rowCount - mapped_rows.length - mapped_invalid_rows.length).toLocaleString()}`);
            console.log(`CSV files created: ${filesCreated.length}`);
            console.log(`${'='.repeat(60)}`);

            if (filesCreated.length > 0) {
                console.log('\nCreated files:');
                filesCreated.forEach((file, idx) => {
                    if (fs.existsSync(file)) {
                        const stats = fs.statSync(file);
                        console.log(`  ${idx + 1}. ${file} (${stats.size} bytes)`);
                    }
                });
            }

            console.log('\n✅ CSV file processing completed.\n');
            resolve({filesCreated, validRows: mapped_rows.length, invalidRows: mapped_invalid_rows.length});
        };
    });
}


async function readFile(source, useMapping = true) {
    // Check if source is a buffer or stream (from S3)
    if (Buffer.isBuffer(source) || (source && source.readable)) {
        console.log('Reading from S3 buffer/stream\n');

        if (useMapping) {
            console.log('Using POSITION-BASED mapping (Array - Optimized!)\n');
        }

        return await readCSVFileWithStreaming(source, useMapping);
    }

    // Otherwise, it's a file path
    const ext = path.extname(source).toLowerCase();

    console.log(`Reading file: ${source}\n`);

    if (ext === '.csv') {
        console.log('Detected CSV file - using CSV parser\n');

        if (useMapping) {
            console.log('Using POSITION-BASED mapping (Array - Optimized!)\n');
        }

        return await readCSVFileWithStreaming(source, useMapping);
    } else {
        throw new Error(`Unsupported file type: ${ext}. Supported types: .csv`);
    }
}


// Execute the function

// ========== EXAMPLE 1: Read from local file ==========
const fileName = 'CSV_SAMPLE_40K.csv';
readFile(fileName)
    .then(async (result) => {
        console.log("result", result)
        // Load CSV files into MySQL database
        if (result && result.filesCreated && result.filesCreated.length > 0) {
            await loadCSVFilesToMySQL(result.filesCreated);
        }
    })
    .catch(error => {
        console.error('Error reading file:', error);
    });


// ========== EXAMPLE 2: Read from S3 buffer ==========
// Uncomment to use with S3:
/*
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

async function readFromS3() {
    try {
        // Get file from S3
        const params = {
            Bucket: 'your-bucket-name',
            Key: 'path/to/your/file.csv'
        };

        const s3Object = await s3.getObject(params).promise();

        // Option 1: Use the buffer directly
        await readFile(s3Object.Body);

        // Option 2: Use the stream
        // const stream = s3.getObject(params).createReadStream();
        // await readFile(stream);

    } catch (error) {
        console.error('Error reading from S3:', error);
    }
}

readFromS3();
*/


class SamplingSourceDataValidationSchema {
    static importValidation = Joi.object({
        // for each transaction, these fields are mandatory
        account_name: Joi.string().required().trim().max(100).label('Account name'),
        account_number: Joi.string().required().trim().max(25).label('Account number'),
        transaction_number: Joi.string().required().max(30).trim().label('Transaction number'),
        amount: Joi.number().required().label('Amount'),
        issue_date: Joi.date().required().label('Issue date').messages({
            'date.base': 'Issue date must be a valid date and should be in chosen format',
        }),
    }).unknown(true); // Allow unknown keys
}