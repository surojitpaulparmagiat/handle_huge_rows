const fs = require('fs');
const path = require('path');
const os = require('os');
const Piscina = require('piscina');
const AWS = require('aws-sdk');
const { defaultProvider } = require("@aws-sdk/credential-provider-node");
require('dotenv').config();
const AWS_DEFAULT_REGION = `${process.env.AWS_DEFAULT_REGION}`;


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


const random_folder_name = Math.random().toString(36).substring(2, 15);

TEMP_FOLDER_SLUG = `${organization_id}/temp/${audit_file_id}/sampling_source_data/${random_folder_name}`;


const maxRows = 10_00_000;
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

// Helper: split a large CSV into N smaller files by row count
async function splitCsvByRows(sourcePath, parts = partCount) {
    const inputFullPath = path.resolve(sourcePath);
    const inputStream = fs.createReadStream(inputFullPath, {encoding: 'utf8'});

    const inputPartsDir = path.join(TEMP_FOLDER, 'input_parts');
    if (!fs.existsSync(inputPartsDir)) {
        fs.mkdirSync(inputPartsDir, {recursive: true});
    }

    const headerLine = await new Promise((resolve, reject) => {
        let header = '';
        inputStream.once('error', reject);
        inputStream.once('data', (chunk) => {
            const str = chunk.toString();
            const idx = str.indexOf('\n');
            if (idx === -1) {
                reject(new Error('CSV header line not found'));
                return;
            }
            header = str.slice(0, idx + 1);
            resolve(header);
        });
    });

    // Reset stream to start for full read
    inputStream.close();
    const stream = fs.createReadStream(inputFullPath, {encoding: 'utf8'});

    const partFiles = [];
    let currentPartIndex = 0;
    let currentLineCount = 0;
    let currentWriteStream = null;

    const openNextPart = () => {
        if (currentWriteStream) {
            currentWriteStream.end();
        }
        const partPath = path.join(inputPartsDir, `part_${currentPartIndex + 1}.csv`);
        currentPartIndex += 1;
        currentLineCount = 0;
        currentWriteStream = fs.createWriteStream(partPath, {encoding: 'utf8'});
        currentWriteStream.write(headerLine);
        partFiles.push(partPath);
    };

    const targetLinesPerPart = Math.ceil(maxRows / parts);

    return new Promise((resolve, reject) => {
        let buffer = '';

        stream.on('error', reject);

        stream.on('data', (chunk) => {
            buffer += chunk;
            let idx;
            while ((idx = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, idx + 1);
                buffer = buffer.slice(idx + 1);

                // Skip header line (first line) when splitting
                if (line.startsWith('description') && currentPartIndex === 0 && currentLineCount === 0) {
                    continue;
                }

                if (!currentWriteStream || currentLineCount >= targetLinesPerPart) {
                    openNextPart();
                }

                currentWriteStream.write(line);
                currentLineCount += 1;
            }
        });

        stream.on('end', () => {
            if (currentWriteStream) {
                currentWriteStream.end();
            }
            resolve(partFiles);
        });
    });
}

// ========== FUNCTION 1: parseAndCreateCSV ==========
async function parseAndCreateCSV(source, options = {}) {
    console.time('parseAndCreateCSV');

    // note: upload source file to temp s3 storage
    await storeFileInTemp(TEMP_FOLDER_SLUG, fs.readFileSync(source), "primary_source_file.csv");

    // read back the file from temp s3 storage
    const fileBuffer = await readFileFromTemp(TEMP_FOLDER_SLUG, "primary_source_file.csv");
    // todo: use this buffer as source for further processing

    // Split the big CSV into multiple parts and process them in parallel
    const parts = await splitCsvByRows(source);

    const tasks = parts.map((partPath, index) =>
        csvPiscina.run({source: partPath, options, workerId: `w${index + 1}`})
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

    // try {
    //     console.time('loadCSVFilesToMySQL');
    //     console.log(`[loadCSVFilesToMySQL] Preparing to load ${csvFiles.length} file(s) into MySQL`);
    //
    //     connection = await mysql.createConnection({
    //         ...dbConfig,
    //         infileStreamFactory: (p) => fs.createReadStream(p),
    //     });
    //
    //     // Simple fast-bulk-load tuning: disable foreign key checks and non-unique indexes
    //     await connection.query('SET GLOBAL local_infile = 1');
    //     await connection.query('SET autocommit = 0');
    //     await connection.query('SET foreign_key_checks = 0');
    //     await connection.query('ALTER TABLE aud_source_data DISABLE KEYS');
    //
    //     let totalRowsInserted = 0;
    //
    //     for (let i = 0; i < csvFiles.length; i++) {
    //         const filePath = csvFiles[i];
    //         const absolutePath = path.resolve(filePath).replace(/\\/g, '/');
    //
    //         const loadDataSQL = `
    //             LOAD DATA LOCAL INFILE '${absolutePath}'
    //             INTO TABLE aud_source_data
    //             FIELDS TERMINATED BY ','
    //             ENCLOSED BY '"'
    //             LINES TERMINATED BY '\n'
    //             IGNORE 1 ROWS
    //             (description, reference_number, issue_date, transaction_number,
    //              account_name, account_number, amount, trial_balance_account_id,
    //              transaction_type, transaction_created_by)
    //             SET source_data_import_session_id = 1, createdAt = NOW(), updatedAt = NOW()
    //         `;
    //
    //         const [result] = await connection.query(loadDataSQL);
    //         const rowsAffected = result.affectedRows || 0;
    //         totalRowsInserted += rowsAffected;
    //     }
    //
    //     await connection.query('ALTER TABLE aud_source_data ENABLE KEYS');
    //     await connection.query('SET foreign_key_checks = 1');
    //     await connection.query('COMMIT');
    //     await connection.query('SET autocommit = 1');
    //
    //     console.log(`[loadCSVFilesToMySQL] Total rows inserted: ${totalRowsInserted}`);
    //     console.timeEnd('loadCSVFilesToMySQL');
    //
    //     return totalRowsInserted;
    // } catch (error) {
    //     console.error('Database error during bulk load:', error);
    //     if (connection) {
    //         try {
    //             await connection.query('ROLLBACK');
    //             await connection.query('SET foreign_key_checks = 1');
    //             await connection.query('ALTER TABLE aud_source_data ENABLE KEYS');
    //             await connection.query('SET autocommit = 1');
    //         } catch (cleanupErr) {
    //             console.error('Error during cleanup after failure:', cleanupErr);
    //         }
    //     }
    //     throw error;
    // } finally {
    //     if (connection) {
    //         await connection.end();
    //     }
    // }
}


// Simple usage example from local CSV file
(async () => {
    try {
        const fileName = 'One_million_rows.csv';
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