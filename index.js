const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const { Readable } = require('stream');
const { DateTime } = require('luxon');
const Joi = require('joi');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const mysql = require('mysql2/promise');

// MySQL Database Configuration
const DB_CONFIG = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'password',
    database: '1audittest',
};

// Position-based mapping configuration (can still be changed dynamically)
const positionMappingArray = [
    'description',
    '',
    'issue_date',
    'transaction_number',
    'account_name',
    'tb_account_code',
    'amount_debit',
    'amount_credit',
    'account_number',
];

const amount_debit_credit_same_column = false;
const maxRows = 10_00_000;
const ROWS_PER_FILE = 25_000;
const TEMP_FOLDER = './temp';

if (!fs.existsSync(TEMP_FOLDER)) {
    fs.mkdirSync(TEMP_FOLDER, { recursive: true });
}

class SamplingSourceDataValidationSchema {
    static importValidation = Joi.object({
        account_name: Joi.string().required().trim().max(100).label('Account name'),
        account_number: Joi.string().required().trim().max(25).label('Account number'),
        transaction_number: Joi.string().required().max(30).trim().label('Transaction number'),
        amount: Joi.number().required().label('Amount'),
        issue_date: Joi.date().required().label('Issue date').messages({
            'date.base': 'Issue date must be a valid date and should be in chosen format',
        }),
    }).unknown(true);
}

// ========== FUNCTION 1: parseAndCreateCSV ==========
async function parseAndCreateCSV(source, options = {}) {
    const {
        useMapping = true,
        posMappingArray = positionMappingArray,
        issueDateFormat = 'dd-MM-yyyy',
    } = options;

    console.time('parseAndCreateCSV');
    console.log('[parseAndCreateCSV] Starting parsing and CSV generation');

    return new Promise((resolve, reject) => {
        let stream;

        if (Buffer.isBuffer(source)) {
            console.log('[parseAndCreateCSV] Using buffer source');
            stream = Readable.from(source);
        } else if (typeof source === 'string') {
            console.log(`[parseAndCreateCSV] Reading from file: ${source}`);
            stream = fs.createReadStream(source);
        } else if (source && source.readable) {
            console.log('[parseAndCreateCSV] Using readable stream source');
            stream = source;
        } else {
            return reject(new Error('Invalid source: must be a file path, Buffer, or readable stream'));
        }

        const mapped_rows = [];
        const mapped_invalid_rows = [];

        let rowCount = 0;
        let batchNumber = 1;
        let filesCreated = [];
        let lastBatchWrittenAt = 0;

        // small inline helpers used only inside this function
        const extractNegativeAuditNumber = (number) => {
            if (number == null || number === '') return 0;
            const str = number.toString().trim();
            if (str.startsWith('(') && str.endsWith(')')) {
                return -parseFloat(str.slice(1, -1).replace(/,/g, ''));
            }
            return parseFloat(str.replace(/,/g, ''));
        };

        const zeroIfEmpty = (value) => {
            if (value == null || value === '' || Number.isNaN(value)) {
                return 0;
            }
            return Number(value);
        };

        const numberParseFunction = (raw_value) => zeroIfEmpty(extractNegativeAuditNumber(raw_value));

        const dateFormatDateISO = (date, df = 'dd/MM/yyyy') => {
            if (!date) return '';
            // Try ISO string first (typical CSV case)
            const iso_date = DateTime.fromISO(date);
            if (iso_date.isValid) return iso_date.toFormat(df);
            // Fallback to JS Date
            const js_date = DateTime.fromJSDate(date);
            if (js_date.isValid) return js_date.toFormat(df);
            return '';
        };

        const dateToDBFormat = (date) => dateFormatDateISO(date, 'yyyy-MM-dd');

        const dateTryParseFromFormat = (date_string, format_string) => {
            const luxon_date = DateTime.fromFormat(date_string, format_string);
            return luxon_date.isValid ? dateToDBFormat(luxon_date) : null;
        };

        const transformRowByPosition = (row) => {
            const values = Object.values(row);

            const transformedRow = {
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

            for (let i = 0; i < values.length; i++) {
                const mappingKey = posMappingArray[i];
                if (mappingKey === '') continue;
                const key = mappingKey || `col_${i}`;
                transformedRow[key] = values[i];
            }

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

            const issue_date_raw = transformedRow.issue_date;
            transformedRow.issue_date = issue_date_raw
                ? (dateTryParseFromFormat(issue_date_raw, issueDateFormat) || '')
                : '';

            delete transformedRow.amount_debit;
            delete transformedRow.amount_credit;

            return transformedRow;
        };

        const toMysqlCsvValue = (value) => {
            if (value === null || value === undefined || value === '') {
                return '\\N';
            }
            return value;
        };

        const writeBatchToCSV = async (rows, batchNo) => {
            if (!rows || rows.length === 0) return null;

            const fileName = `CSV_${batchNo}.csv`;
            const filePath = path.join(TEMP_FOLDER, fileName);

            console.log(`[parseAndCreateCSV] Writing batch ${batchNo} with ${rows.length} rows to ${fileName}`);

            const csvWriter = createCsvWriter({
                path: filePath,
                header: [
                    { id: 'description', title: 'description' },
                    { id: 'reference_number', title: 'reference_number' },
                    { id: 'issue_date', title: 'issue_date' },
                    { id: 'transaction_number', title: 'transaction_number' },
                    { id: 'account_name', title: 'account_name' },
                    { id: 'account_number', title: 'account_number' },
                    { id: 'amount', title: 'amount' },
                    { id: 'trial_balance_account_id', title: 'trial_balance_account_id' },
                    { id: 'transaction_type', title: 'transaction_type' },
                    { id: 'transaction_created_by', title: 'transaction_created_by' },
                ],
            });

            const safeRows = rows.map((r) => ({
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
        };

        const writeInvalidRowsToCSV = async (invalidRows) => {
            if (!invalidRows || invalidRows.length === 0) return null;

            const fileName = 'INVALID_ROWS.csv';
            const filePath = path.join(TEMP_FOLDER, fileName);

            const csvWriter = createCsvWriter({
                path: filePath,
                header: [
                    { id: 'row_number', title: 'row_number' },
                    { id: 'error_reason', title: 'error_reason' },
                ],
            });

            const formattedRows = invalidRows.map((item) => ({
                row_number: item.rowNumber,
                error_reason: item.error,
            }));

            await csvWriter.writeRecords(formattedRows);
            return filePath;
        };

        const printSummary = async () => {
            if (printSummary.printed) return;
            printSummary.printed = true;

            await new Promise((resolveDelay) => setTimeout(resolveDelay, 200));

            const remainingRows = mapped_rows.length - lastBatchWrittenAt;
            if (remainingRows > 0) {
                const filePath = await writeBatchToCSV(
                    mapped_rows.slice(lastBatchWrittenAt),
                    batchNumber,
                );
                if (filePath) filesCreated.push(filePath);
            }

            if (mapped_invalid_rows.length > 0) {
                console.log(`[parseAndCreateCSV] Writing ${mapped_invalid_rows.length} invalid rows to INVALID_ROWS.csv`);
                await writeInvalidRowsToCSV(mapped_invalid_rows);
            }

            const summary = {
                filesCreated,
                validRows: mapped_rows.length,
                invalidRows: mapped_invalid_rows.length,
            };

            console.log('[parseAndCreateCSV] Completed parsing');
            console.log('[parseAndCreateCSV] Summary:', summary);
            console.timeEnd('parseAndCreateCSV');

            resolve(summary);
        };

        stream
            .pipe(csv())
            .on('data', (row) => {
                if (rowCount < maxRows) {
                    rowCount++;

                    const outputRow = useMapping ? transformRowByPosition(row) : row;

                    const { error } = SamplingSourceDataValidationSchema.importValidation.validate(
                        outputRow,
                        {
                            allowUnknown: true,
                            abortEarly: false,
                            errors: {
                                escapeHtml: false,
                                wrap: { label: '' },
                            },
                        },
                    );

                    if (error) {
                        mapped_invalid_rows.push({
                            rowNumber: rowCount,
                            row: outputRow,
                            error: error.details.map((d) => d.message).join(', '),
                        });
                    } else {
                        mapped_rows.push(outputRow);
                    }

                    const validRowsSinceLastWrite = mapped_rows.length - lastBatchWrittenAt;

                    if (validRowsSinceLastWrite >= ROWS_PER_FILE) {
                        const currentBatchNumber = batchNumber;
                        const batchStart = lastBatchWrittenAt;
                        const batchEnd = mapped_rows.length;

                        lastBatchWrittenAt = batchEnd;
                        batchNumber++;

                        const batchToWrite = mapped_rows.slice(batchStart, batchEnd);
                        writeBatchToCSV(batchToWrite, currentBatchNumber)
                            .then((filePath) => {
                                if (filePath) filesCreated.push(filePath);
                            })
                            .catch((err) => {
                                console.error(`Error writing batch ${currentBatchNumber}:`, err);
                            });
                    }
                }

                if (rowCount >= maxRows) {
                    stream.destroy();
                }
            })
            .on('end', () => {
                printSummary();
            })
            .on('error', (error) => {
                reject(error);
            });

        stream.on('close', () => {
            printSummary();
        });
    });
}

// ========== FUNCTION 2: loadCSVFilesToMySQL ==========
async function loadCSVFilesToMySQL(csvFiles, dbConfig = DB_CONFIG)  {
    if (!csvFiles || csvFiles.length === 0) {
        console.log('[loadCSVFilesToMySQL] No CSV files to load');
        return 0;
    }

    let connection;
    try {
        console.time('loadCSVFilesToMySQL');
        console.log(`[loadCSVFilesToMySQL] Preparing to load ${csvFiles.length} file(s) into MySQL`);

        connection = await mysql.createConnection({
            ...dbConfig,
            infileStreamFactory: (p) => fs.createReadStream(p),
        });

        await connection.query('SET foreign_key_checks = 0');
        await connection.query('SET unique_checks = 0');
        await connection.query('SET autocommit = 0');

        let totalRowsInserted = 0;

        for (let i = 0; i < csvFiles.length; i++) {
            const filePath = csvFiles[i];
            const absolutePath = path.resolve(filePath).replace(/\\/g, '/');

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
            } catch (err) {
                console.error(`Error loading ${path.basename(filePath)}:`, err.message);
            }
        }

        await connection.query('COMMIT');
        await connection.query('SET foreign_key_checks = 1');
        await connection.query('SET unique_checks = 1');
        await connection.query('SET autocommit = 1');

        console.log(`[loadCSVFilesToMySQL] Total rows inserted: ${totalRowsInserted}`);
        console.timeEnd('loadCSVFilesToMySQL');

        return totalRowsInserted;
    } catch (error) {
        console.error('Database error:', error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

// Simple usage example from local CSV file
(async () => {
    try {
        const fileName = 'CSV_SAMPLE_40K.csv';
        const parseResult = await parseAndCreateCSV(fileName);

        if (parseResult && parseResult.filesCreated && parseResult.filesCreated.length > 0) {
            await loadCSVFilesToMySQL(parseResult.filesCreated);
        }
    } catch (error) {
        console.error('Error in processing pipeline:', error);
    }
})();

module.exports = {
    parseAndCreateCSV,
    loadCSVFilesToMySQL,
};
