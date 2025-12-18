const fs = require('fs');
const path = require('path');
const os = require('os');
const Piscina = require('piscina');
const AWS = require('aws-sdk');
const { defaultProvider } = require("@aws-sdk/credential-provider-node");
require('dotenv').config();
const AWS_DEFAULT_REGION = `${process.env.AWS_DEFAULT_REGION}`;


AWS.config.update({
    defaultProvider,
    region: AWS_DEFAULT_REGION,
});
const AWS_S3_SERVICE = (bucket_access_type = 'private') => {
    const S3_REGION = `${process.env.S3_REGION}`;
    const S3_BUCKET_NAME_PRIVATE = `${process.env.S3_BUCKET_NAME_PRIVATE}`;
    const S3_BUCKET_NAME_PUBLIC = `${process.env.S3_BUCKET_NAME_PUBLIC}`;

    return new AWS.S3({
        region: S3_REGION,
        params: { Bucket: ('private' === bucket_access_type ? S3_BUCKET_NAME_PRIVATE : S3_BUCKET_NAME_PUBLIC) },
        signatureVersion: 'v4',
    });
}
const storeFileInTemp = async (folder_slug, file_buffer, file_name) => {
    try {
        const OneAuditS3 = AWS_S3_SERVICE("private");
        const params = {
            Key: `upload/${TEMP_FOLDER_SLUG}/${file_name}`,
            Body: file_buffer,
        };
        console.log("params", params, file_name, TEMP_FOLDER_SLUG )
        await OneAuditS3.upload(params).promise();
    } catch (err) {
        console.error('Error uploading file:', err);
        throw new Error('Error uploading file to temp storage');
    }

}
const readFileFromTemp = async (folder_slug, file_name) => {

    try {
        const OneAuditS3 = AWS_S3_SERVICE("private");
        const params = {
            Key: `upload/${folder_slug}/${file_name}`,
        };
        const response_data = await OneAuditS3.getObject(params).promise();
        return response_data.Body;
    } catch (err) {
        console.error('Error uploading file:', err);
        throw Error('Error reading file from temp storage');
    }


};









// MySQL Database Configuration
const DB_CONFIG = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'password',
    database: '1audittest',
};
const organization_id = 10000; // todo: dynamic value
const audit_file_id =15000; // todo: dynamic value


const random_folder_name = 100   //Math.random().toString(36).substring(2, 15);

TEMP_FOLDER_SLUG = `${organization_id}/temp/${audit_file_id}/sampling_source_data/${random_folder_name}`;


const TEMP_FOLDER = './temp';

if (!fs.existsSync(TEMP_FOLDER)) {
    fs.mkdirSync(TEMP_FOLDER, {recursive: true});
}

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

    // 4) Dispatch to workers
    const tasks = slices.map((sliceBuf, index) =>
        csvPiscina.run({
            source: sliceBuf,
            options,
            workerId: `w_${index + 1}`,
        })
    );

    const results = await Promise.all(tasks);

    // Merge worker results
    const merged = {
        filesCreated: [],
        validRows: 0,
        invalidRows: 0,
    };

    for (const r of results) {
        if (!r) continue;
        if (Array.isArray(r.filesCreated)) {
            merged.filesCreated.push(...r.filesCreated);
        }
        merged.validRows += r.validRows || 0;
        merged.invalidRows += r.invalidRows || 0;
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
        await connection.query('SET GLOBAL local_infile = 1');
        await connection.query('SET autocommit = 0');
        await connection.query('SET foreign_key_checks = 0');
        await connection.query('ALTER TABLE aud_source_data DISABLE KEYS');

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

            const [result] = await connection.query(loadDataSQL);
            const rowsAffected = result.affectedRows || 0;
            totalRowsInserted += rowsAffected;
        }

        await connection.query('ALTER TABLE aud_source_data ENABLE KEYS');
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
                await connection.query('ALTER TABLE aud_source_data ENABLE KEYS');
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
        const localFileName =  'One_million_rows.csv';

        // Upload local file once to temp S3 storage under a known key
        // const localBuffer = fs.readFileSync(localFileName);
        const s3Key = 'primary_source_file.csv';


        // await storeFileInTemp(TEMP_FOLDER_SLUG, localBuffer, s3Key);

        // todo: try to read the file.
        // console.log("file uploaded to temp S3 storage.");


        // Now parse from S3 (no local disk reads inside workers)
        const parseResult = await parseAndCreateCSV(s3Key);

        // if (parseResult && parseResult.filesCreated && parseResult.filesCreated.length > 0) {
        //     await loadCSVFilesToMySQL(parseResult.filesCreated);
        // }
    } catch (error) {
        console.error('Error in processing pipeline:', error);
    }
})();

module.exports = {
    parseAndCreateCSV,
    loadCSVFilesToMySQL,
};
