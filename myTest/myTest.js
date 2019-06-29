const AWS = require('aws-sdk');
const dotenv = require('dotenv');
const envResult = dotenv.config();
if (envResult.error) throw envResult.error;
const s3 = require('../lib/index');
const AwsS3 = new AWS.S3({
    region: process.env.AWS_REGION
});

const s3Client = new s3.Client({
    maxAsync: 20,
    s3RetryCount: 3,
    s3RetryDelay: 1000,
    multipartUploadThreshold: 10 * 1024 * 1024, // 10MB
    multipartUploadSize: 5 * 1024 * 1024, // 5MB
    s3Client: AwsS3
});

let upload = s3Client.uploadFile({
    localFile: 'c:/testing/FileGen_39156203.mp4',
    s3Params: {
        Bucket: process.env.S3_BUCKET,
        Key: 'FileGen_39156203.mp4'
    }
});

upload.on('error', err => {
    console.error('uploader error', err);
});

upload.on('fileClosed', () => {
    console.log(upload.localFile, 'closed');
});

upload.on('fileOpened', () => {
    console.log(upload.localFile, 'opened');
});

upload.on('end', () => {
    console.log(upload.localFile, 'end');
});

upload.on('progress', () => {
    console.log(
        upload.localFile,
        'progress:',
        upload.progressAmount,
        '/',
        upload.progressTotal,
        '=',
        Math.round((upload.progressAmount / upload.progressTotal) * 10000) / 100,
        '%'
    );
});
