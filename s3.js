const AWS = require('aws-sdk');
const {defaultProvider} = require('@aws-sdk/credential-provider-node');
require('dotenv').config();

const AWS_DEFAULT_REGION = `${process.env.AWS_DEFAULT_REGION}`;
const S3_REGION = `${process.env.S3_REGION}`;
const S3_BUCKET_NAME_PRIVATE = `${process.env.S3_BUCKET_NAME_PRIVATE}`;
const S3_BUCKET_NAME_PUBLIC = `${process.env.S3_BUCKET_NAME_PUBLIC}`;
const UPLOAD = process.env.UPLOAD !== '0';

console.log("UPLOAD", UPLOAD)

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

async function storeFileInTemp(folder_slug, body, file_name) {
    const OneAuditS3 = AWS_S3_SERVICE('private');
    const key = `upload/${folder_slug}/${file_name}`;
    console.log("file upload started ... ", key)
    if (UPLOAD) {
        await OneAuditS3.upload({Key: key, Body: body}).promise();
    }
    console.log("file upload end ... ", key)
    return key;
}

async function readFileFromTemp(folder_slug, file_name) {
    const OneAuditS3 = AWS_S3_SERVICE('private');
    const key = `upload/${folder_slug}/${file_name}`;
    const res = await OneAuditS3.getObject({Key: key}).promise();
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
