'use strict';

let   AWS = require('aws-sdk')
  , digestStream = require('digest-stream')
  , EventEmitter = require('events').EventEmitter
  , fd_slicer = require('fd-slicer')
  , fs = require('fs-extra')
  , mime = require('mime')
  , MultipartETag = require('./multipart_etag')
  , Pend = require('pend')
  , progBar = require('progbar')
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
exports.MultipartETag = MultipartETag;
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
  let bar;
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
        bar = new progBar.ProgressBar({filename: s3Params.Key, size: localFileStat.size});
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
      console.dir(data, {depth: null, colors: true});
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
        let buff;
        let errorOccured = false;
        inStream.on('error', function(err){
          if(fatalError || errorOccured) return;
          handleError(err);
        });
        s3Params.ContentLength = end - start;
        s3Params.PartNumber = part.PartNumber;
        s3Params.UploadId = uploadId;

        localFileSlicer.read(buff, 0, s3Params.ContentLength, start, function(err, bytesRead, buffer){
          console.err('Err:', err);
          console.log('bytesRead', bytesRead);
          console.log('buffer', buffer);
        })
        let multipartETag = new MultipartETag({size: s3Params.ContentLength, count: 1});
        let prevBytes = 0;
        let overallDelta = 0;
        let pend = new Pend();
        let haveETag = pend.hold();
        multipartETag.on('progress', function(){
          if(fatalError || errorOccured) return;
          let delta = multipartETag.bytes - prevBytes;
          prevBytes = multipartETag.bytes;
          uploader.progressAmount += delta;
          overallDelta += delta;
          uploader.emit('progress');
        });
        multipartETag.on('end', function(){
          if(fatalError || errorOccured) return;
          let delta = multipartETag.bytes - prevBytes;
          uploader.progressAmount += delta;
          uploader.progressTotal += (end - start) - multipartETag.bytes;
          uploader.emit('progress');
          haveETag();
        });
        inStream.pipe(multipartETag);
        s3Params.Body = multipartETag;

        if(!uploadedParts[part.PartNumber]){
          console.log('Uploading Missing Part:', part.PartNumber);
          self.s3.uploadPart(extend({}, s3Params), function(err, data){
            pendCb();
            if(fatalError || errorOccured) return;
            if(err){
              errorOccured = true;
              uploader.progressAmount -= overallDelta;
              cb(err);
              return;
            }
            pend.wait(function(){
              if(fatalError) return;
              if(!compareMultipartETag(data.ETag, multipartETag)){
                errorOccured = true;
                uploader.progressAmount -= overallDelta;
                cb(new Error('ETag does not match MD5 checksum'));
                return;
              }
              part.ETag = data.ETag;

              uploadedParts[part.PartNumber] = uploadedParts[part.PartNumber] ? uploadedParts[part.PartNumber] : {};
              uploadedParts[part.PartNumber].ETag = data.ETag;
              uploadedParts[part.PartNumber].Size = s3Params.ContentLength;
              uploadedBytes = uploadedBytes + s3Params.ContentLength;
              console.log('Bytes Uploaded', uploadedBytes);
              bar.advance(uploadedBytes);
              cb(null, data);
              return;
            });
          });
        } else {
          if(fatalError) return;
          if(!compareMultipartETag(uploadedParts[part.PartNumber].ETag, multipartETag)){
            console.log('Uploading Missing Part BC MD5 does not match:', part.PartNumber);
            //console.log('PartNumber:', part.PartNumber, '\nUploadedPart ETag:', uploadedParts[part.PartNumber].ETag, '\nComputed ETag:', multipartETag );
            self.s3.uploadPart(extend({}, s3Params), function(err, data){
              pendCb();
              if(fatalError || errorOccured) return;
              if(err){
                errorOccured = true;
                uploader.progressAmount -= overallDelta;
                cb(err);
                return;
              }
              pend.wait(function(){
                if(fatalError) return;
                if(!compareMultipartETag(data.ETag, multipartETag)){
                  errorOccured = true;
                  uploader.progressAmount -= overallDelta;
                  cb(new Error('ETag does not match MD5 checksum'));
                  return;
                }
                part.ETag = data.ETag;

                uploadedParts[part.PartNumber] = uploadedParts[part.PartNumber] ? uploadedParts[part.PartNumber] : {};
                uploadedParts[part.PartNumber].ETag = data.ETag;
                uploadedParts[part.PartNumber].Size = s3Params.ContentLength;
                uploadedBytes = uploadedBytes + s3Params.ContentLength;
                console.log('Bytes Uploaded', uploadedBytes);
                bar.advance(uploadedBytes);
                cb(null, data);
                return;
              });
            });
          } else {
            console.log('Skipping Part', part.PartNumber);
            console.log('Digest', digest, 'length', length);
            part.ETag = uploadedParts[part.PartNumber].ETag;
            uploadedBytes = uploadedBytes + s3Params.ContentLength;
            console.log('Bytes Uploaded', uploadedBytes);
            bar.advance(uploadedBytes);
            cb(null, {ETag: `"${uploadedParts[part.PartNumber].ETag}"`});
          }
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
      bar.end();
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
      inStream.on('error', handleError);
      let pend = new Pend();
      let multipartETag = new MultipartETag({size: localFileStat.size, count: 1});
      pend.go(function(cb){
        multipartETag.on('end', function(){
          if(fatalError) return;
          uploader.progressAmount = multipartETag.bytes;
          uploader.progressTotal = multipartETag.bytes;
          localFileStat.size = multipartETag.bytes;
          localFileStat.multipartETag = multipartETag;
          bar.end();
          uploader.emit('progress');
          cb();
        });
      });
      multipartETag.on('progress', function(){
        if(fatalError) return;
        uploader.progressAmount = multipartETag.bytes;
        bar.advance(uploader.progressAmount);
        uploader.emit('progress');
      });
      s3Params.ContentLength = localFileStat.size;
      uploader.progressAmount = 0;
      inStream.pipe(multipartETag);
      s3Params.Body = multipartETag;

      self.s3.putObject(s3Params, function(err, data){
        pendCb();
        if(fatalError) return;
        if(err){
          cb(err);
          return;
        }
        pend.wait(function(){
          if(fatalError) return;
          if(!compareMultipartETag(data.ETag, localFileStat.multipartETag)){
            cb(new Error('ETag does not match MD5 checksum'));
            return;
          }
          cb(null, data);
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

function cleanETag(eTag) {
  return eTag ? eTag.replace(/^\s*'?\s*"?\s*(.*?)\s*"?\s*'?\s*$/, "$1") : "";
}

function compareMultipartETag(eTag, multipartETag) {
  return multipartETag.anyMatch(cleanETag(eTag));
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