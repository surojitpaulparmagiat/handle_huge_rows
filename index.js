const fs = require('fs');
const path = require('path');
const os = require('os');
const Piscina = require('piscina');
const mysql = require('mysql2/promise');
const { createObjectCsvStringifier } = require('csv-writer');
const { storeFileInTemp, readFileFromTemp, s3FullUri} = require('./s3');
require('dotenv').config();

const db_user = process.env.MYSQL_USER;
const db_password = process.env.MYSQL_PASSWORD;
const db_host = process.env.MYSQL_WRITE_HOST;
const db_name = process.env.MYSQL_DATABASE;

// MySQL Database Configuration
const DB_CONFIG = {
    host: db_host,
    port: 3306,
    user: db_user,
    password: db_password,
    database: db_name,
};

console.log("DB_CONFIG", DB_CONFIG)

const table_name = 'temp_aud_source_data'; // todo: dynamic value
const organization_id = 10000; // todo: dynamic value
const audit_file_id =15000; // todo: dynamic value


const random_folder_name = 100   //Math.random().toString(36).substring(2, 15);

TEMP_FOLDER_SLUG = `${organization_id}/temp/${audit_file_id}/sampling_source_data/${random_folder_name}`;



// Initialize a Piscina worker pool for CSV parsing
const cpuCount = os.cpus().length || 1;
const partCount = Math.max(2, Math.floor(cpuCount / 2));

const csvPiscina = new Piscina({
    filename: path.resolve(__dirname, 'csvWorker.js'),
    minThreads: Math.max(1, partCount),
    maxThreads: cpuCount,
});

// ========== FUNCTION: splitCsvByRows ==========
// Split a full CSV file buffer into `parts` slices on newline boundaries.
// Each returned Buffer is prefixed with the original header line so every slice is a valid CSV.
function splitCsvByRows(fileBuffer, parts) {
    if (!fileBuffer || !fileBuffer.length || parts <= 1) {
        return [fileBuffer];
    }

    const totalLength = fileBuffer.length;

    // Find the end of the header (first \n). If not found, treat whole buffer as a single part.
    let headerEnd = -1;
    for (let i = 0; i < totalLength; i++) {
        if (fileBuffer[i] === 0x0a /* \n */) { headerEnd = i; break; }
    }
    if (headerEnd === -1) {
        return [fileBuffer];
    }

    const header = fileBuffer.slice(0, headerEnd + 1); // include the newline
    const body = fileBuffer.slice(headerEnd + 1);

    if (body.length === 0) {
        // Only header present
        return [header];
    }

    const bytesPerPart = Math.ceil(body.length / parts);

    const slices = [];
    let offset = 0;
    while (offset < body.length) {
        let tentativeEnd = offset + bytesPerPart;
        if (tentativeEnd >= body.length) {
            tentativeEnd = body.length;
        } else {
            // advance to next newline so we don't cut a line in half
            let advanced = false;
            for (let i = tentativeEnd; i < body.length; i++) {
                if (body[i] === 0x0a /* \n */) { tentativeEnd = i + 1; advanced = true; break; }
            }
            if (!advanced) tentativeEnd = body.length;
        }
        const bodySlice = body.slice(offset, tentativeEnd);
        // Prefix with header to make this a standalone CSV chunk
        const combined = Buffer.concat([header, bodySlice]);
        slices.push(combined);
        offset = tentativeEnd;
    }

    return slices;
}

// ========== FUNCTION 1: parseAndCreateCSV (S3-based) ==========
async function parseAndCreateCSV(sourceKey, options = {}) {

    console.time("readFileFromTemp");
    // 1) Read full file buffer from S3 temp storage (already uploaded)
    const fileBuffer = await readFileFromTemp(TEMP_FOLDER_SLUG, sourceKey);
    console.timeEnd("readFileFromTemp");

    console.time('parseAndCreateCSV');
    // 2) Decide how many parallel workers to use
    const cpuCountLocal = os.cpus().length || 1;
    const partCountLocal = Math.max(2, Math.floor(cpuCountLocal / 2));

    if (!fileBuffer || fileBuffer.length === 0) {
        console.timeEnd('parseAndCreateCSV');
        return { filesCreated: [], validRows: 0, invalidRows: 0 };
    }

    // 3) Split buffer into newline-aligned slices, each with header
    const slices = splitCsvByRows(fileBuffer, partCountLocal);
    console.log("slices", slices)

    // 4) Dispatch to workers
    const tasks = slices.map((sliceBuf, index) =>
        csvPiscina.run({
            source: sliceBuf,
            options,
            workerId: `w_${index + 1}`,
            folderSlug: `${TEMP_FOLDER_SLUG}/db_csv`, // note: a subfolder for db_csv files
        })
    );

    const results = await Promise.all(tasks);
    console.log("results", results)

    // Merge worker results
    const merged = {
        filesCreated: [],
        validRows: 0,
        invalidRows: 0,
        invalidRowsFile: null,
    };

    const allInvalidRows = [];

    for (const r of results) {
        if (!r) continue;
        if (Array.isArray(r.filesCreated)) {
            merged.filesCreated.push(...r.filesCreated);
        }
        merged.validRows += r.validRows || 0;
        merged.invalidRows += r.invalidRows || 0;

        // Collect invalid rows data from all workers
        if (r.invalidRowsData && Array.isArray(r.invalidRowsData) && r.invalidRowsData.length > 0) {
            allInvalidRows.push(...r.invalidRowsData);
        }
    }

    // Write a single consolidated invalid rows file to S3
    if (allInvalidRows.length > 0) {
        const fileName = 'INVALID_ROWS.csv';

        const formattedRows = allInvalidRows.map((item) => ({
            row_number: item.rowNumber,
            error_reason: item.error,
        }));

        const stringifier = createObjectCsvStringifier({
            header: [
                { id: 'row_number', title: 'row_number' },
                { id: 'error_reason', title: 'error_reason' },
            ],
        });

        const csvContent = stringifier.getHeaderString() + stringifier.stringifyRecords(formattedRows);
        const buffer = Buffer.from(csvContent, 'utf8');

        await storeFileInTemp(TEMP_FOLDER_SLUG, buffer, fileName);
        merged.invalidRowsFile = `upload/${TEMP_FOLDER_SLUG}/${fileName}`;
        console.log(`Created consolidated invalid rows file: ${merged.invalidRowsFile}`);
    }

    console.timeEnd('parseAndCreateCSV');
    return merged;
}

// ========== FUNCTION 2: loadCSVFilesToMySQL ==========
async function loadCSVFilesToMySQL(csvFiles, dbConfig = DB_CONFIG) {
    console.log("csvFiles ..", csvFiles)
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




        // Simple fast-bulk-load tuning: disable foreign key checks and non-unique indexes
        // await connection.query('SET GLOBAL local_infile = 1');
        await connection.query('SET autocommit = 0');
        await connection.query('SET foreign_key_checks = 0');
        await connection.query(`ALTER TABLE ${table_name} DISABLE KEYS`);

        let totalRowsInserted = 0;

        for (let i = 0; i < csvFiles.length; i++) {
            const filePath = csvFiles[i];
            const s3_uri = s3FullUri(filePath);
            console.log(`[loadCSVFilesToMySQL] Loading file ${i + 1}/${csvFiles.length}: ${s3_uri}`);

            const loadDataSQL = `
                LOAD DATA FROM S3 '${s3_uri}'
                INTO TABLE ${table_name}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (description, reference_number, issue_date, transaction_number,
                 account_name, account_number, amount, trial_balance_account_id,
                 transaction_type, transaction_created_by)
                SET source_data_import_session_id = 1, createdAt = NOW(), updatedAt = NOW()
            `;

            const [result] = await connection.query(loadDataSQL);
            const rowsAffected = result.affectedRows || 0;
            totalRowsInserted += rowsAffected;
        }

        await connection.query(`ALTER TABLE ${table_name} ENABLE KEYS`);
        await connection.query('SET foreign_key_checks = 1');
        await connection.query('COMMIT');
        await connection.query('SET autocommit = 1');

        console.log(`[loadCSVFilesToMySQL] Total rows inserted: ${totalRowsInserted}`);
        console.timeEnd('loadCSVFilesToMySQL');

        return totalRowsInserted;
    } catch (error) {
        console.error('Database error during bulk load:', error);
        if (connection) {
            try {
                await connection.query('ROLLBACK');
                await connection.query('SET foreign_key_checks = 1');
                await connection.query(`ALTER TABLE ${table_name} ENABLE KEYS`);
                await connection.query('SET autocommit = 1');
            } catch (cleanupErr) {
                console.error('Error during cleanup after failure:', cleanupErr);
            }
        }
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}



// Simple usage example from S3 temp storage
(async () => {
    try {
        const localFileName = '10k_rows.csv' ; // 'One_million_rows.csv';
        const s3Key = 'primary_source_file.csv';

        // Upload local file once to temp S3 storage under a known key
        // const localBuffer = fs.readFileSync(localFileName);
        // await storeFileInTemp(TEMP_FOLDER_SLUG, localBuffer, s3Key);


        // Now parse from S3 (no local disk reads inside workers)
        const parseResult = await parseAndCreateCSV(s3Key);
        console.log('Parse result:', parseResult);

        if (parseResult && parseResult.filesCreated && parseResult.filesCreated.length > 0) {
            console.log(`Created ${parseResult.filesCreated.length} valid CSV file(s) in S3`);
            console.log(`Valid rows: ${parseResult.validRows}`);
            console.log(`Invalid rows: ${parseResult.invalidRows}`);
            if (parseResult.invalidRowsFile) {
                console.log(`Invalid rows file: ${parseResult.invalidRowsFile}`);
            }
            await loadCSVFilesToMySQL(parseResult.filesCreated);
        }
    } catch (error) {
        console.error('Error in processing pipeline:', error);
    }
})();

module.exports = {
    parseAndCreateCSV,
    loadCSVFilesToMySQL,
    storeFileInTemp,
    TEMP_FOLDER_SLUG,
};
