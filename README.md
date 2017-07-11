# High Level Amazon S3 Client

## Installation

`npm install s3 --save`

## Features

 * Automatically retry a configurable number of times when S3 returns an error.
 * Includes logic to make multiple requests when there is a 1000 object limit.
 * Ability to set a limit on the maximum parallelization of S3 requests.
   Retries get pushed to the end of the parallelization queue.
 * Ability to sync a dir to and from S3.
 * Progress reporting.
 * Supports files of any size (up to S3's maximum 5 TB object size limit).
 * Uploads large files quickly using parallel multipart uploads.
 * Uses heuristics to compute multipart ETags client-side to avoid uploading
   or downloading files unnecessarily.
 * Automatically provide Content-Type for uploads based on file extension.
 * Support third-party S3-compatible platform services like Ceph

See also the companion CLI tool which is meant to be a drop-in replacement for
s3cmd: [s3-cli](https://github.com/andrewrk/node-s3-cli).

## Synopsis

### Create a client

```js
var s3 = require('s3');

var client = s3.createClient({
  maxAsyncS3: 20,     // this is the default
  s3RetryCount: 3,    // this is the default
  s3RetryDelay: 1000, // this is the default
  multipartUploadThreshold: 20971520, // this is the default (20 MB)
  multipartUploadSize: 15728640, // this is the default (15 MB)
  s3Options: {
    accessKeyId: "your s3 key",
    secretAccessKey: "your s3 secret",
    region: "your region",
    // endpoint: 's3.yourdomain.com',
    // sslEnabled: false
    // any other options are passed to new AWS.S3()
    // See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property
  },
});
```

### Create a client from existing AWS.S3 object

```js
var s3 = require('s3');
var awsS3Client = new AWS.S3(s3Options);
var options = {
  s3Client: awsS3Client,
  // more options available. See API docs below.
};
var client = s3.createClient(options);
```

### Upload a file to S3

```js
var params = {
  localFile: "some/local/file",

  s3Params: {
    Bucket: "s3 bucket name",
    Key: "some/remote/file",
    // other options supported by putObject, except Body and ContentLength.
    // See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property
  },
};
var uploader = client.uploadFile(params);
uploader.on('error', function(err) {
  console.error("unable to upload:", err.stack);
});
uploader.on('progress', function() {
  console.log("progress", uploader.progressMd5Amount,
            uploader.progressAmount, uploader.progressTotal);
});
uploader.on('end', function() {
  console.log("done uploading");
});
```

## API Documentation

### s3.AWS

This contains a reference to the aws-sdk module. It is a valid use case to use
both this module and the lower level aws-sdk module in tandem.

### s3.createClient(options)

Creates an S3 client.

`options`:

 * `s3Client` - optional, an instance of `AWS.S3`. Leave blank if you provide `s3Options`.
 * `s3Options` - optional. leave blank if you provide `s3Client`.
   - See AWS SDK documentation for available options which are passed to `new AWS.S3()`:
     http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property
 * `maxAsyncS3` - maximum number of simultaneous requests this client will
   ever have open to S3. defaults to `20`.
 * `s3RetryCount` - how many times to try an S3 operation before giving up.
   Default 3.
 * `s3RetryDelay` - how many milliseconds to wait before retrying an S3
   operation. Default 1000.
 * `multipartUploadThreshold` - if a file is this many bytes or greater, it
   will be uploaded via a multipart request. Default is 20MB. Minimum is 5MB.
   Maximum is 5GB.
 * `multipartUploadSize` - when uploading via multipart, this is the part size.
   The minimum size is 5MB. The maximum size is 5GB. Default is 15MB. Note that
   S3 has a maximum of 10000 parts for a multipart upload, so if this value is
   too small, it will be ignored in favor of the minimum necessary value
   required to upload the file.

### client.uploadFile(params)

See http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property

`params`:

 * `s3Params`: params to pass to AWS SDK `putObject`.
 * `localFile`: path to the file on disk you want to upload to S3.
 * (optional) `defaultContentType`: Unless you explicitly set the `ContentType`
   parameter in `s3Params`, it will be automatically set for you based on the
   file extension of `localFile`. If the extension is unrecognized,
   `defaultContentType` will be used instead. Defaults to
   `application/octet-stream`.

The difference between using AWS SDK `putObject` and this one:

 * This works with files, not streams or buffers.
 * If the reported MD5 upon upload completion does not match, it retries.
 * If the file size is large enough, uses multipart upload to upload parts in
   parallel.
 * Retry based on the client's retry settings.
 * Progress reporting.
 * Sets the `ContentType` based on file extension if you do not provide it.

Returns an `EventEmitter` with these properties:

 * `progressMd5Amount`
 * `progressAmount`
 * `progressTotal`

And these events:

 * `'error' (err)`
 * `'end' (data)` - emitted when the file is uploaded successfully
   - `data` is the same object that you get from `putObject` in AWS SDK
 * `'progress'` - emitted when `progressMd5Amount`, `progressAmount`, and
   `progressTotal` properties change. Note that it is possible for progress to
   go backwards when an upload fails and must be retried.
 * `'fileOpened' (fdSlicer)` - emitted when `localFile` has been opened. The file
   is opened with the [fd-slicer](https://github.com/andrewrk/node-fd-slicer)
   module because we might need to read from multiple locations in the file at
   the same time. `fdSlicer` is an object for which you can call
   `createReadStream(options)`. See the fd-slicer README for more information.
 * `'fileClosed'` - emitted when `localFile` has been closed.

And these methods:

 * `abort()` - call this to stop the find operation.

## Testing

`S3_KEY=<valid_s3_key> S3_SECRET=<valid_s3_secret> S3_BUCKET=<valid_s3_bucket> npm test`

Tests upload and download large amounts of data to and from S3. The test
timeout is set to 40 seconds because Internet connectivity waries wildly.
