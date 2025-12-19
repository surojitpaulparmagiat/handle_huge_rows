const fs = require('fs');
const path = require('path');
const os = require('os');
const Piscina = require('piscina');
const { createObjectCsvStringifier } = require('csv-writer');
const { storeFileInTemp, readFileFromTemp, s3FullUri} = require('./s3');
require('dotenv').config();
const { parse } = require('@fast-csv/parse');

// --- minimal time logging helpers ---
function ts() { return new Date().toISOString(); }
function log(msg) { console.log(`[${ts()}] ${msg}`); }
function since(startMs) { return `${Date.now() - startMs}ms`; }

const source_data_table_name = 'temp_aud_source_data';
const import_session_table_name = 'temp_aud_source_data_import_sessions';



const organization_id = 10000; // todo: dynamic value
const audit_file_id =15000; // todo: dynamic value
const client_entity_id = 20000; // todo: dynamic value
const audit_period = 5; // todo: dynamic value
const user_id = 1; // todo: dynamic value


const localFileName = '10K_rows.csv';




const { getDbConnection } = require('./db_connection');



const createImportSessionId = async ({})=>{
    const query = `INSERT INTO ${import_session_table_name}
    (organization_id, audit_file_id, client_entity_id, audit_period, updated_by, createdAt, updatedAt, progress)
    VALUES (?, ?, ?, ?, ?, NOW(), NOW(), JSON_OBJECT('total',0, 'processed',0))`;
    const conn = await getDbConnection();
    try {
        const [result] = await conn.execute(query, [
            organization_id,
            audit_file_id,
            client_entity_id,
            audit_period,
            user_id,
            user_id,
        ]);
        return result.insertId;
    } catch (error) {
        throw error;
    }
    finally {
        if (conn) {
            await conn.end();
        }
    }
}

const updateImportSessionProgress = async ({sessionId, total, processed, conn  })=>{
    const query = `UPDATE ${import_session_table_name}
    SET progress = JSON_OBJECT('total', ?, 'processed', ?), updatedAt = NOW()
    WHERE id = ?`;
    try {
        await conn.execute(query, [
            total,
            processed,
            sessionId,
        ]);
    } catch (error) {
        console.log("Error updating import session progress:", error);
    }

}





const csvPiscina = new Piscina({
    filename: path.resolve(__dirname, 'csvWorker.js'),
    minThreads:2,
    maxThreads: 2,
});

// ========== FUNCTION: splitCsvByRows ==========


// Split using fast-csv to parse rows correctly and then produce CSV chunk buffers with header
async function splitCsvByRowsFast(fileBuffer, parts) {
    if (!fileBuffer || !fileBuffer.length || parts <= 1) {
        return [fileBuffer];
    }

    // Determine header line (first line) by scanning to first \n
    let headerEnd = -1;
    for (let i = 0; i < fileBuffer.length; i++) {
        if (fileBuffer[i] === 0x0a /* \n */) { headerEnd = i; break; }
    }
    if (headerEnd === -1) {
        return [fileBuffer];
    }

    const headerBuf = fileBuffer.slice(0, headerEnd + 1);
    const bodyBuf = fileBuffer.slice(headerEnd + 1);

    // Parse rows using fast-csv
    const rows = await new Promise((resolve, reject) => {
        const collected = [];
        const stream = parse({ headers: false, ignoreEmpty: true, trim: true })
            .on('error', reject)
            .on('data', (row) => collected.push(row))
            .on('end', () => resolve(collected));
        stream.write(bodyBuf);
        stream.end();
    });


    if (rows.length === 0) {
        return [headerBuf];
    }

    const partsClamped = Math.max(1, Math.min(parts, rows.length));
    const targetRowsPerPart = Math.ceil(rows.length / partsClamped);

    // Reconstruct CSV chunks with header + rows
    const slices = [];
    let idx = 0;
    while (idx < rows.length) {
        const endIdx = Math.min(rows.length, idx + targetRowsPerPart);
        const chunkRows = rows.slice(idx, endIdx);
        // Convert rows back to CSV lines
        const lines = chunkRows.map((cols) => cols.map((c) => {
            // naive escape: wrap in quotes if contains comma or quote; double any internal quotes
            const s = String(c ?? '');
            const needsQuote = s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r');
            if (needsQuote) {
                return '"' + s.replace(/"/g, '""') + '"';
            }
            return s;
        }).join(',')).join('\n');
        const withHeader = Buffer.concat([headerBuf, Buffer.from(lines + '\n', 'utf8')]);
        slices.push(withHeader);
        idx = endIdx;
    }
    console.log("slices", slices)

    return slices;
}

// ========== FUNCTION 1: parseAndCreateCSV (S3-based) ==========
async function parseAndCreateCSV(sourceKey, options = {}, DB_CSV_FOLDER_SLUG, PRIMARY_FILE_FOLDER_SLUG) {
    const tAll = Date.now();
    log(`parseAndCreateCSV: start (sourceKey=${sourceKey})`);

    // 1) Read full file buffer from S3 temp storage (already uploaded)
    const tRead = Date.now();
    const fileBuffer = await readFileFromTemp(PRIMARY_FILE_FOLDER_SLUG, sourceKey);
    log(`readFileFromTemp done in ${since(tRead)} (bytes=${fileBuffer ? fileBuffer.length : 0})`);

    // 2) Decide how many parallel workers to use (fixed to 4)
    const partsToUse = 2;

    if (!fileBuffer || fileBuffer.length === 0) {
        log(`parseAndCreateCSV: empty buffer, returning`);
        return { filesCreated: [], validRows: 0, invalidRows: 0 };
    }

    // 3) Split buffer into newline-aligned slices, each with header
    const tSplit = Date.now();
    const slices = await splitCsvByRowsFast(fileBuffer, partsToUse);
    log(`splitCsvByRows -> ${slices.length} slices in ${since(tSplit)}`);

    // Sanity check: verify total rows preserved (count of \n, excluding header lines)
    try {
        const countNewlines = (buf) => {
            let c = 0; for (let i = 0; i < buf.length; i++) { if (buf[i] === 0x0a) c++; } return c;
        };
        const totalRows = Math.max(0, countNewlines(fileBuffer) - 1);
        let slicedRows = 0;
        for (const s of slices) {
            slicedRows += Math.max(0, countNewlines(s) - 1);
        }
        log(`row-count check: original=${totalRows}, after-split=${slicedRows}`);
    } catch (_) {}
    // 4) Dispatch to workers
    const tWorkers = Date.now();
    const tasks = slices.map((sliceBuf, index) =>
        csvPiscina.run({
            source: sliceBuf,
            options,
            workerId: `w_${index + 1}`,
            folderSlug: `${DB_CSV_FOLDER_SLUG}/db_csv`, // note: a subfolder for db_csv files
        })
    );

    const results = await Promise.all(tasks);
    log(`worker processing finished in ${since(tWorkers)}`);

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
        const tInvalid = Date.now();
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

        await storeFileInTemp(DB_CSV_FOLDER_SLUG, buffer, fileName, true);
        log(`INVALID_ROWS.csv written (${formattedRows.length} rows) in ${since(tInvalid)}`);
        merged.invalidRowsFile = `upload/${DB_CSV_FOLDER_SLUG}/${fileName}`;
    }

    log(`parseAndCreateCSV: done in ${since(tAll)} (validRows=${merged.validRows}, invalidRows=${merged.invalidRows}, filesCreated=${merged.filesCreated.length})`);
    return merged;
}

// ========== FUNCTION 2: loadCSVFilesToMySQL ==========
async function loadCSVFilesToMySQL(csvFiles, sessionId ) {
    if (!csvFiles || csvFiles.length === 0) {
        return { inserted: 0, warnings: [] };
    }

    const connection = await getDbConnection()
    // Create a separate connection for progress updates with autocommit ON
    const progressConn = await getDbConnection();
    await progressConn.query('SET autocommit = 1');

    const tAll = Date.now();
    log(`loadCSVFilesToMySQL: start (files=${csvFiles.length})`);

    const warningsLog = [];

    try {

        // Initialize progress using the progress connection
        await updateImportSessionProgress({
            sessionId: sessionId,
            total: csvFiles.length,
            processed: 0,
            conn: progressConn,
        }).catch(() => {});

        await connection.query('SET autocommit = 0');
        await connection.query('SET foreign_key_checks = 0');
        // removed MyISAM-specific DISABLE KEYS

        let totalRowsInserted = 0;

        for (let i = 0; i < csvFiles.length; i++) {
            const filePath = csvFiles[i];
            const s3_uri = s3FullUri(filePath);

            const loadDataSQL = `
                LOAD DATA FROM S3 '${s3_uri}'
                INTO TABLE ${source_data_table_name}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (description, reference_number, issue_date, transaction_number,
                 account_name, account_number, amount, trial_balance_account_id,
                 transaction_type, transaction_created_by)
                SET source_data_import_session_id = 1, createdAt = NOW(), updatedAt = NOW()
            `;

            const tOne = Date.now();
            const [result] = await connection.query(loadDataSQL);
            const rowsAffected = result.affectedRows || 0;
            totalRowsInserted += rowsAffected;
            log(`Loaded file ${i + 1}/${csvFiles.length} from S3 in ${since(tOne)} (+${rowsAffected} rows)`);

            // Update import session progress using separate autocommit connection
            try {
                await updateImportSessionProgress({
                    sessionId: sessionId,
                    total: csvFiles.length,
                    processed: i + 1,
                    conn: progressConn,
                });
            } catch (e) {
                console.log('Progress update failed for file', i + 1, e?.message || e);
            }

            if (result && result.warningStatus && result.warningStatus > 0) {
                const tWarn = Date.now();
                const [warnings] = await connection.query('SHOW WARNINGS LIMIT 50');
                log(`MySQL warnings (${warnings.length}) for file ${i + 1}: in ${since(tWarn)}`);
                for (const w of warnings) {
                    warningsLog.push({ file_name: filePath, message: w.Message });
                }
            }

        }

        // removed MyISAM-specific ENABLE KEYS
        await connection.query('SET foreign_key_checks = 1');
        await connection.query('COMMIT');
        await connection.query('SET autocommit = 1');

        log(`loadCSVFilesToMySQL: committed in ${since(tAll)} (totalRowsInserted=${totalRowsInserted}, warnings=${warningsLog.length})`);
        return { inserted: totalRowsInserted, warnings: warningsLog };
    }
    catch (error) {
        if (connection) {
            try {
                await connection.query('ROLLBACK');
                await connection.query('SET foreign_key_checks = 1');
                await connection.query('SET autocommit = 1');
            } catch (cleanupErr) {}
        }
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
        if (progressConn) {
            try { await progressConn.end(); } catch (_) {}
        }
    }
}



// Simple usage example from S3 temp storage
(async () => {
    const tAll = Date.now();
    try {
        const s3Key = 'primary_source_file.csv';
        log('Script start');

        const sessionId = await createImportSessionId({});
        log(`Created import session ID: ${sessionId}`);

        const random_folder_name = `S${sessionId}`
        const tUpload = Date.now();
        const localBuffer = fs.readFileSync(localFileName);

        const PRIMARY_FILE_FOLDER_SLUG =  `${organization_id}/temp/${audit_file_id}/sampling_source_data`;
        // Upload local file once to temp S3 storage under a known key
        await storeFileInTemp(PRIMARY_FILE_FOLDER_SLUG, localBuffer, s3Key, true);
        log(`Uploaded local file to temp S3 as ${s3Key} in ${since(tUpload)}`);

        // Now parse from S3 (no local disk reads inside workers)
        const tParse = Date.now();
        const DB_CSV_FOLDER_SLUG = `${organization_id}/temp/${audit_file_id}/sampling_source_data/${random_folder_name}`;
        const parseResult = await parseAndCreateCSV(s3Key,{}, DB_CSV_FOLDER_SLUG, PRIMARY_FILE_FOLDER_SLUG);
        log(`parseAndCreateCSV finished in ${since(tParse)}`);
        //
        if (parseResult && parseResult.filesCreated && parseResult.filesCreated.length > 0) {
            const tLoad = Date.now();
            const { inserted, warnings } = await loadCSVFilesToMySQL(parseResult.filesCreated, sessionId);
            log(`loadCSVFilesToMySQL finished in ${since(tLoad)} (inserted=${inserted})`);
            if (warnings && warnings.length) {
               await saveWarningsToS3(warnings, DB_CSV_FOLDER_SLUG);
            }
        }
    } catch (error) {
        console.error('Error:', error);
    } finally {
        log(`Script end in ${since(tAll)}`);
    }
})();


const saveWarningsToS3 = async (warnings, DB_CSV_FOLDER_SLUG) => {
    // stringify warnings and save as JSON in S3
    const warningsJson = JSON.stringify(warnings, null, 2);
    const warningsBuffer = Buffer.from(warningsJson, 'utf8');
    const warningsFileName = `WARNINGS.json`;
    await storeFileInTemp(DB_CSV_FOLDER_SLUG, warningsBuffer, warningsFileName, true);
    log(`Warnings JSON uploaded to S3 at upload/${DB_CSV_FOLDER_SLUG}/${warningsFileName}`);
}


module.exports = {
    storeFileInTemp,
};
