'use strict';

let   AWS = require('aws-sdk')
  , crypto = require('crypto')
  , EventEmitter = require('events').EventEmitter
  , fd_slicer = require('fd-slicer')
  , fs = require('fs-extra')
  , mime = require('mime')
  , Pend = require('pend')
  , s3 = new AWS.S3()
  ;

let debug = true;

var MAX_PUTOBJECT_SIZE = 5 * 1024 * 1024 * 1024;
var MAX_DELETE_COUNT = 1000;
var MAX_MULTIPART_COUNT = 10000;
var MIN_MULTIPART_SIZE = 5 * 1024 * 1024;

exports.createClient = function(options){
  return new Client(options);
}

exports.Client = Client;
exports.AWS = AWS;

exports.MAX_PUTOBJECT_SIZE = MAX_PUTOBJECT_SIZE;
exports.MAX_DELETE_COUNT = MAX_DELETE_COUNT;
exports.MAX_MULTIPART_COUNT = MAX_MULTIPART_COUNT;
exports.MIN_MULTIPART_SIZE = MIN_MULTIPART_SIZE;

function Client(options){
  options = options ? options : {};
  this.s3 = options.s3Client || new AWS.S3(options.s3Options);
  this.s3Pend = new Pend();
  this.s3Pend.max = options.maxAsyncs3 || 20;
  this.s3RetryCount = options.s3RetryCount || 3;
  this.s3RetryDelay = options.s3RetryDelay || 1000;
  this.multipartUploadThreshold = options.multipartUploadThreshold || (20 * 1024 * 1024);
  this.multipartUploadSize = options.multipartUploadSize || (15 * 1024 * 1024);
  this.multipartDownloadThreshold = options.multipartDownloadThreshold || (20 * 1024 * 1024);
  this.multipartDownloadSize = options.multipartDownloadSize || (15 * 1024 * 1024);

  if(this.multipartUploadThreshold < MIN_MULTIPART_SIZE){
    throw new Error('Minimum multipartUploadThreshold is 5MB.');
  }
  if(this.multipartUploadThreshold > MAX_PUTOBJECT_SIZE){
    throw new Error('Maximum multipartUploadThreshold is 5GB.');
  }
  if(this.multipartUploadSize < MIN_MULTIPART_SIZE){
    throw new Error('Minimum multipartUploadSize is 5MB.');
  }
  if(this.multipartUploadSize > MAX_PUTOBJECT_SIZE){
    throw new Error('Maximum multipartUploadSize is 5GB.');
  }
}

Client.prototype.uploadFile = function(params){
  let self = this;
  let uploader = new EventEmitter();
  uploader.setMaxListeners(0);
  uploader.progressMd5Amount = 0;
  uploader.progressAmount = 0;
  uploader.progressTotal = 0;
  uploader.abort = handleAbort;

  let localFile = params.localFile;
  let localFileStat = null;
  
  params.s3Params.Key = encodeSpecialCharacters(params.s3Params.Key);

  let s3Params = extend({}, params.s3Params);
  if(s3Params.ContentType === undefined){
    let defaultContentType = params.defaultContentType || 'application/octet-stream';
    s3Params.ContentType = mime.lookup(localFile, defaultContentType);
  }
  let fatalError = false;
  let localFileSlicer = null;
  let parts = [];
  let uploadedParts = {};
  let uploadId;
  let uploadedBytes = 0;

  console.log(`Uploading s3://${s3Params.Bucket}/${s3Params.Key}`);
  openFile();

  return uploader;

  function handleError(err) {
    if (localFileSlicer) {
      localFileSlicer.unref();
      localFileSlicer = null;
    }
    if (fatalError) return;
    fatalError = true;
    uploader.emit('error', err);
  }

  function handleAbort() {
    fatalError = true;
  }

  function openFile(){
    fs.open(localFile, 'r', function(err, fd){
      if (err) return handleError(err);
      localFileSlicer = fd_slicer.createFromFd(fd, {autoClose: true});
      localFileSlicer.on('error', handleError);
      localFileSlicer.on('close', function(){
        uploader.emit('fileClosed');
      });

      // keep an extra reference alive untill we decide that we're completely done with the file
      localFileSlicer.ref();

      uploader.emit('fileOpened', localFileSlicer);

      fs.fstat(fd, function(err, stat){
        if(err) return handleError(err);
        localFileStat = stat;
        uploader.progressTotal = stat.size;
        startPuttingObject();
      });
    });
  }

  function startPuttingObject(){
    if(localFileStat.size >= self.multipartUploadThreshold){
      let multipartUploadSize = self.multipartUploadSize;
      let partsRequiredCount = Math.ceil(localFileStat.size / multipartUploadSize);
      if(partsRequiredCount > MAX_MULTIPART_COUNT){
        multipartUploadSize = smallestPartSizeFromFileSize(localFileStat.size);
      }
      if(multipartUploadSize > MAX_PUTOBJECT_SIZE){
        let err = new Error(`File size exceeds maximum object size: ${localFile}`);
        err.retryable = false;
        handleError(err);
        return;
      }
      testMultipartUpload(multipartUploadSize);
    } else {
      doWithRetry(tryPuttingObject, self.s3RetryCount, self.s3RetryDelay, onPutObjectDone);
    }

    function onPutObjectDone(err, data){
      if(fatalError) return;
      if(err) return handleError(err);
      if(localFileSlicer){
        localFileSlicer.unref();
        localFileSlicer = null;
      }
      uploader.emit('end', data);
    }
  }

  function testMultipartUpload(multipartUploadSize){
    doWithRetry(tryTestMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data){
      if(fatalError) return;
      if(err) return handleError(err);
      if(data.Uploads.length === 0){
        startMultipartUpload(multipartUploadSize);
      } else {
        uploadId = data.Uploads[0].UploadId;
        startResumeMultipartUpload(data.Uploads[0]);
      }
    });
  }

  function tryTestMultipartUpload(cb){
    if(fatalError) return;
    self.s3Pend.go(function(pendCb){
      if(fatalError) return pendCb();
      self.s3.listMultipartUploads({Bucket: s3Params.Bucket, Prefix: s3Params.Key}, function(err, data){
        pendCb();
        if(fatalError) return;
        cb(err, data);
      });
    });
  }

  function startResumeMultipartUpload(currentUpload){
    doWithRetry(tryStartResumeMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data){
      if(fatalError) return;
      if(err) return handleError(err);
      uploader.emit('resume', data);
      s3Params = {
        Bucket: s3Params.Bucket,
        Key: s3Params.Key,
        SSECustomerAlgorithm: s3Params.SSECustomerAlgorithm,
        SSECustomerKey: s3Params.SSECustomerKey,
        SSECustomerKeyMD5: s3Params.SSECustomerKeyMD5
      };
      for (var i = 0; i < data.Parts.length; i++) {
        uploadedParts[data.Parts[i].PartNumber] = {};
        uploadedParts[data.Parts[i].PartNumber].LastModified = data.Parts[i].LastModified;
        uploadedParts[data.Parts[i].PartNumber].ETag = data.Parts[i].ETag;
        uploadedParts[data.Parts[i].PartNumber].Size = data.Parts[i].Size;
      }
      console.log('Resume        ', data.UploadId, data.Parts[0].Size);
      queueNeededParts(data.UploadId, data.Parts[0].Size);
    });
  }

  function tryStartResumeMultipartUpload(cb){
    if(fatalError) return;
    self.s3Pend.go(function(pendCb){
      if(fatalError) return pendCb();
      // list Parts already uploaded
      self.s3.listParts({Bucket: s3Params.Bucket, Key: s3Params.Key, UploadId: uploadId}, function(err, data){
        pendCb();
        if(fatalError) return;
        cb(err, data);
      });
    });
  }

  function startMultipartUpload(multipartUploadSize){
    doWithRetry(tryCreateMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data){
      if(fatalError) return;
      if(err) return handleError(err);
      uploader.emit('data', data);
      s3Params = {
        Bucket: s3Params.Bucket,
        Key: s3Params.Key,
        SSECustomerAlgorithm: s3Params.SSECustomerAlgorithm,
        SSECustomerKey: s3Params.SSECustomerKey,
        SSECustomerKeyMD5: s3Params.SSECustomerKeyMD5
      };
      console.log('Start         ', data.UploadId, multipartUploadSize);
      queueNeededParts(data.UploadId, multipartUploadSize);
    });
  }

  function tryCreateMultipartUpload(cb){
    if(fatalError) return;
    self.s3Pend.go(function(pendCb){
      if(fatalError) return;
      self.s3.createMultipartUpload(s3Params, function(err, data){
        pendCb();
        if(fatalError) return;
        cb(err, data);
      });
    });
  }

  function queueNeededParts(uploadId, multipartUploadSize){
    let cursor = 0;
    let nextPartNumber = 1;
    let pend = new Pend();
    console.log('              ', uploadId, multipartUploadSize);
    while (cursor < localFileStat.size){
      let start = cursor;
      let end = cursor + multipartUploadSize;
      if(end > localFileStat.size){
        end = localFileStat.size;
      }
      cursor = end;

      let part = {
        ETag: null,
        PartNumber: nextPartNumber++
      };
      parts.push(part);
      uploader.progressAmount = 0;
      pend.go(makeUploadPart(start, end, part, uploadId));
    }
    pend.wait(function(err){
      if(fatalError) return;
      if(err) return handleError(err);
      completeMultipartUpload();
    });
  }

  function makeUploadPart(start, end, part, uploadId){
    return function(cb){
      doWithRetry(tryUploadPart, self.s3RetryCount, self.s3RetryDelay, function(err, data){
        if(fatalError) return;
        if(err) return handleError(err);
        uploader.emit('part', part);
        cb();
      });
    };

    function tryUploadPart(cb){
      if(fatalError) return;
      self.s3Pend.go(function(pendCb){
        if(fatalError){
          pendCb();
          return;
        }
        let inStream = localFileSlicer.createReadStream({start: start, end: end});
        let errorOccured = false;
        s3Params.ContentLength = end - start;
        s3Params.PartNumber = part.PartNumber;
        s3Params.UploadId = uploadId;
        let buffer = new Buffer(s3Params.ContentLength);
        let md5;
        let pend = new Pend();
        inStream.on('error', function(err){
          if(fatalError || errorOccured) return;
          handleError(err);
        });
        pend.go(function(goCb){
          localFileSlicer.read(buffer, 0, s3Params.ContentLength, start, function(err, bytesRead, buffer){
            if(fatalError) return;
            md5 = calcMD5(buffer);
            goCb();
          });
        });

        s3Params.Body = inStream;

        if(!uploadedParts[part.PartNumber]){
          if(fatalError) return;
          self.s3.uploadPart(extend({}, s3Params), function(err, data){
            pendCb();
            if(fatalError || errorOccured) return;
            if(err){
              errorOccured = true;
              uploader.progressAmount -= s3Params.ContentLength;
              cb(err);
              return;
            }
            pend.wait(function(){
              if(fatalError) return;
              if(cleanETag(data.ETag) !== md5){
                errorOccured = true;
                uploader.progressAmount -= s3Params.ContentLength;
                cb(new Error('ETag does not match MD5 checksum'));
                return;
              }
              part.ETag = data.ETag;

              uploadedParts[part.PartNumber] = uploadedParts[part.PartNumber] ? uploadedParts[part.PartNumber] : {};
              uploadedParts[part.PartNumber].ETag = data.ETag;
              uploadedParts[part.PartNumber].Size = s3Params.ContentLength;
              uploader.progressAmount += s3Params.ContentLength;
              cb(null, data);
              return;
            });
          });
        } else {
          if(fatalError) return;
          pend.wait(function(){
            if(fatalError) return;
            if(cleanETag(uploadedParts[part.PartNumber].ETag) !== md5){
              self.s3.uploadPart(extend({}, s3Params), function(err, data){
                pendCb();
                if(fatalError || errorOccured) return;
                if(err){
                  errorOccured = true;
                  uploader.progressAmount -= s3Params.ContentLength;
                  cb(err);
                  return;
                }
                if(cleanETag(data.ETag) !== md5){
                  errorOccured = true;
                  uploader.progressAmount -= s3Params.ContentLength;
                  cb(new Error('ETag does not match MD5 checksum'));
                  return;
                }
                part.ETag = data.ETag;

                uploadedParts[part.PartNumber] = uploadedParts[part.PartNumber] ? uploadedParts[part.PartNumber] : {};
                uploadedParts[part.PartNumber].ETag = data.ETag;
                uploadedParts[part.PartNumber].Size = s3Params.ContentLength;
                uploadedBytes += s3Params.ContentLength;
                cb(null, data);
                return;
              });
            } else {
              pendCb();
              part.ETag = uploadedParts[part.PartNumber].ETag;
              uploader.progressAmount += s3Params.ContentLength;
              cb(null, {ETag: `"${uploadedParts[part.PartNumber].ETag}"`});
            }
          });
        }
      });
    }
  }

  function completeMultipartUpload(){
    localFileSlicer.unref();
    localFileSlicer = null;
    doWithRetry(tryCompleteMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data){
      if(fatalError) return;
      if(err) return handleError(err);
      uploader.emit('end', data);
    });
  }

  function tryCompleteMultipartUpload(cb){
    if(fatalError) return;
    self.s3Pend.go(function(pendCb){
      if(fatalError){
        pendCb();
        return;
      }
      s3Params = {
        Bucket: s3Params.Bucket,
        Key: s3Params.Key,
        UploadId: s3Params.UploadId,
        MultipartUpload: {
          Parts: parts
        }
      };
      self.s3.completeMultipartUpload(s3Params, function(err, data){
        pendCb();
        if(fatalError) return;
        cb(err, data);
      });
    });
  }

  function tryPuttingObject(cb){
    self.s3Pend.go(function(pendCb){
      if(fatalError) return pendCb();
      let inStream = localFileSlicer.createReadStream();
      uploader.progressAmount = 0;
      s3Params.ContentLength = localFileStat.size;
      let buffer = new Buffer(s3Params.ContentLength);
      let md5;
      let pend = new Pend();
      inStream.on('error', handleError);
      pend.go(function(goCb){
        localFileSlicer.read( buffer, 0, s3Params.ContentLength, 0, function(err, bytesRead, buffer){
          if(fatalError) return;
          md5 = calcMD5(buffer);
          goCb();
        })
      });
      s3Params.Body = inStream;
      self.s3.putObject(s3Params, function(err, data){
        pendCb();
        if(fatalError) return;
        if(err){
          cb(err);
          return;
        }
        pend.wait(function(){
          if(fatalError) return;
          if(cleanETag(data.ETag) !== md5){
            cb(new Error('ETag does not match MD5 checksum'));
            return;
          } else {
            uploader.progressAmount += s3Params.ContentLength;
            uploader.progressTotal = s3Params.ContentLength;
            localFileStat.size = uploader.progressTotal;
            uploader.emit('progress');
            cb(null, data);
          }
        });
      });
    });
  }
}

function doWithRetry(fn, tryCount, delay, cb){
  var tryIndex = 0;

  tryOnce();

  function tryOnce(){
    fn(function(err, result){
      if (err){
        if (err.retryable === false) {
          cb(err);
        } else {
          tryIndex += 1;
          if (tryIndex >= tryCount) {
            cb(err);
          } else {
            setTimeout(tryOnce, delay);
          }
        }
      } else {
        cb(null, result);
      }
    });
  }
}

function extend(target, source){
  for (var propName in source){
    target[propName] = source[propName];
  }
  return target;
}

function calcMD5(buffer){
  return crypto.createHash('md5').update(buffer).digest('hex').toString('hex');
}

function cleanETag(eTag) {
  return eTag ? eTag.replace(/^\s*'?\s*"?\s*(.*?)\s*"?\s*'?\s*$/, "$1") : "";
}

function encodeSpecialCharacters(filename) {
  // Note: these characters are valid in URIs, but S3 does not like them for
  // some reason.
  return encodeURI(filename).replace(/[!'()* ]/g, function (char) {
    return '%' + char.charCodeAt(0).toString(16);
  });
}

function smallestPartSizeFromFileSize(fileSize){
  var partSize = Math.ceil(fileSize / MAX_MULTIPART_COUNT);
  return (partSize < MIN_MULTIPART_SIZE) ? MIN_MULTIPART_SIZE : partSize;
}