const {Readable} = require('stream');
const fs = require('fs');
const {DateTime} = require('luxon');
const Joi = require('joi');
const { createObjectCsvStringifier } = require('csv-writer');
const fastCsv = require('@fast-csv/parse');
const { storeFileInTemp } = require('./s3');

// minimal time logging helpers
function ts() { return new Date().toISOString(); }
function log(msg) { console.log(`[${ts()}] ${msg}`); }
function since(startMs) { return `${Date.now() - startMs}ms`; }

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
const ROWS_PER_FILE = 1_00_000;

class SamplingSourceDataValidationSchema {
    static importValidation = Joi.object({
        account_name: Joi.string().required().trim().max(100).label('Account name').messages({
            'string.empty': 'Account name is not allowed to be empty',
            'any.required': 'Account name is required',
            'string.max': 'Account name must be less than or equal to 100 characters',
        }),
        account_number: Joi.string().required().trim().max(25).label('Account number').messages({
            'string.empty': 'Account number is not allowed to be empty',
            'any.required': 'Account number is required',
            'string.max': 'Account number must be less than or equal to 25 characters',
        }),
        // transaction_number: Joi.string().required().max(30).trim().label('Transaction number').messages({
        //     'string.empty': 'Transaction number is not allowed to be empty',
        //     'any.required': 'Transaction number is required',
        //     'string.max': 'Transaction number must be less than or equal to 30 characters',
        // }),
        amount: Joi.number().required().label('Amount').messages({
            'number.base': 'Amount must be a number',
            'any.required': 'Amount is required',
        }),
        issue_date: Joi.date().required().label('Issue date').messages({
            'date.base': 'Issue date must be a valid date and should be in chosen format',
            'any.required': 'Issue date is required',
        }),
    }).unknown(true);
}

async function parseAndCreateCSVWorker({source, options = {}, workerId = '', folderSlug}) {
    const {
        useMapping = true,
        posMappingArray = positionMappingArray,
        issueDateFormat = 'dd-MM-yyyy',
    } = options;

    const MAX_INVALID_ROWS = 10_000;
    const VALIDATION_SAMPLE_LIMIT = 10_000;

    const workerPrefix = workerId ? `${workerId}_` : '';

    const tWorker = Date.now();
    log(`worker ${workerId || 'unknown'}: start`);

    return new Promise((resolve, reject) => {
        let stream;

        // Normalize Uint8Array / ArrayBufferView to a Buffer
        if (source instanceof Uint8Array && !Buffer.isBuffer(source)) {
            source = Buffer.from(source);
        }

        if (Buffer.isBuffer(source)) {
            // Buffer: turn into a readable stream for fast-csv
            stream = Readable.from(source);
        } else if (typeof source === 'string') {
            // File path: create a file stream
            stream = fs.createReadStream(source, {highWaterMark: 1024 * 1024});
        } else if (source && source.readable) {
            // Already a readable stream
            stream = source;
        } else {
            return reject(new Error('Invalid source: must be a file path, Buffer/Uint8Array, or readable stream'));
        }

        const mapped_rows = [];
        const mapped_invalid_rows = [];

        let rowCount = 0;
        let batchNumber = 1;
        let filesCreated = [];
        let lastBatchWrittenAt = 0;
        const pendingWrites = [];

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
            const iso_date = DateTime.fromISO(date);
            if (iso_date.isValid) return iso_date.toFormat(df);
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

            const fileName = `${workerPrefix}CSV_${batchNo}.csv`;

            const tWrite = Date.now();
            // Build safe rows for CSV
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

            // Use in-memory CSV stringifier and upload directly to S3
            const stringifier = createObjectCsvStringifier({
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

            const csvContent = stringifier.getHeaderString() + stringifier.stringifyRecords(safeRows);
            const buffer = Buffer.from(csvContent, 'utf8');

            // Upload to S3 under the temp folder slug and return the S3 key
            await storeFileInTemp(folderSlug, buffer, fileName);
            log(`worker ${workerId || 'unknown'}: wrote ${fileName} (${rows.length} rows) in ${since(tWrite)}`);

            return `upload/${folderSlug}/${fileName}`;
        };

        const printSummary = async () => {
            if (printSummary.printed) return;
            printSummary.printed = true;

            await new Promise((resolveDelay) => setTimeout(resolveDelay, 200));

            // Ensure all in-flight batch writes complete before finalizing
            try { await Promise.all(pendingWrites); } catch (e) { console.error('Error awaiting pending batch writes:', e); }

            const remainingRows = mapped_rows.length - lastBatchWrittenAt;

            if (remainingRows > 0) {

                const filePath = await writeBatchToCSV(
                    mapped_rows.slice(lastBatchWrittenAt),
                    batchNumber,
                );
                if (filePath) filesCreated.push(filePath);
            }

            const summary = {
                filesCreated,
                validRows: mapped_rows.length,
                invalidRows: mapped_invalid_rows.length,
                invalidRowsData: mapped_invalid_rows, // Return actual invalid rows data
            };

            log(`worker ${workerId || 'unknown'}: end in ${since(tWorker)} (valid=${summary.validRows}, invalid=${summary.invalidRows}, files=${filesCreated.length})`);
            resolve(summary);
        };

        stream
            .pipe(fastCsv.parse({
                headers: true,
                ignoreEmpty: false,
                trim: false,
            }))
            .on('error', (error) => {
                reject(error);
            })
            .on('data', (row) => {
                rowCount++;
                const outputRow = useMapping ? transformRowByPosition(row) : row;

                const validateThisRow = rowCount <= VALIDATION_SAMPLE_LIMIT;
                if (validateThisRow) {
                    const {error} = SamplingSourceDataValidationSchema.importValidation.validate(
                        outputRow,
                        {
                            allowUnknown: true,
                            abortEarly: true,
                        },
                    );

                    if (error) {
                        if (mapped_invalid_rows.length < MAX_INVALID_ROWS) {
                            mapped_invalid_rows.push({
                                rowNumber: rowCount,
                                row: undefined,
                                error: error.message,
                            });
                        }
                        return;
                    }
                }

                mapped_rows.push(outputRow);


                const validRowsSinceLastWrite = mapped_rows.length - lastBatchWrittenAt;
                if (validRowsSinceLastWrite >= ROWS_PER_FILE) {
                    // console.log("batchNumber: ", batchNumber);
                    // console.log("mapped_rows count: ", mapped_rows.length);

                    const currentBatchNumber = batchNumber;
                    const batchStart = lastBatchWrittenAt;
                    const batchEnd = mapped_rows.length;

                    lastBatchWrittenAt = batchEnd;
                    batchNumber++;

                    const batchToWrite = mapped_rows.slice(batchStart, batchEnd);
                    const p = writeBatchToCSV(batchToWrite, currentBatchNumber)
                        .then((filePath) => {
                            if (filePath) filesCreated.push(filePath);
                        })
                        .catch((err) => {
                            console.error(`Error writing batch ${currentBatchNumber}:`, err);
                        });
                    pendingWrites.push(p);
                }
            })
            .on('end', () => {
                printSummary();
            });
    });
}

module.exports = parseAndCreateCSVWorker;
