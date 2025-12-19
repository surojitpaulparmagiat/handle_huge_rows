const AWS = require('aws-sdk');
const {defaultProvider} = require('@aws-sdk/credential-provider-node');
require('dotenv').config();

const AWS_DEFAULT_REGION = `${process.env.AWS_DEFAULT_REGION}`;
const S3_REGION = `${process.env.S3_REGION}`;
const S3_BUCKET_NAME_PRIVATE = `${process.env.S3_BUCKET_NAME_PRIVATE}`;
const S3_BUCKET_NAME_PUBLIC = `${process.env.S3_BUCKET_NAME_PUBLIC}`;

// minimal time logging helpers
function ts() { return new Date().toISOString(); }
function log(msg) { console.log(`[${ts()}] ${msg}`); }
function since(startMs) { return `${Date.now() - startMs}ms`; }
const upload= true; // set to false to skip actual uploads

AWS.config.update({
    defaultProvider,
    region: AWS_DEFAULT_REGION,
});

function AWS_S3_SERVICE(bucket_access_type = 'private') {

    return new AWS.S3({
        region: S3_REGION,
        params: {Bucket: bucket_access_type === 'private' ? S3_BUCKET_NAME_PRIVATE : S3_BUCKET_NAME_PUBLIC},
        signatureVersion: 'v4',
    });
}

async function storeFileInTemp(folder_slug, body, file_name, ) {
    const OneAuditS3 = AWS_S3_SERVICE('private');
    const key = `upload/${folder_slug}/${file_name}`;

    const t = Date.now();
    if (upload) {
        await OneAuditS3.upload({Key: key, Body: body}).promise();
        // log(`storeFileInTemp: uploaded ${key} in ${since(t)} (bytes=${body ? body.length : 0})`);
    } else {
        log(`storeFileInTemp: skip upload for ${key}`);
    }

    return key;
}

async function readFileFromTemp(folder_slug, file_name) {
    const OneAuditS3 = AWS_S3_SERVICE('private');
    const key = `upload/${folder_slug}/${file_name}`;
    const t = Date.now();
    const res = await OneAuditS3.getObject({Key: key}).promise();
    // log(`readFileFromTemp: fetched ${key} in ${since(t)} (bytes=${res && res.Body ? res.Body.length : 0})`);

    return res.Body;
}

function s3FullUri(file_path) {
    return `s3://${S3_BUCKET_NAME_PRIVATE}/${file_path}`;
}

module.exports = {
    storeFileInTemp,
    readFileFromTemp,
    s3FullUri,
};
