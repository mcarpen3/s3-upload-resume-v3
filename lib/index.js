const AWS = require('aws-sdk');
const EventEmitter = require('events').EventEmitter;
const fs = require('graceful-fs');
const url = require('url');
const rimraf = require('rimraf');
const findit = require('findit2');
const Pend = require('pend');
const path = require('path');
const crypto = require('crypto');
const mkdirp = require('mkdirp');
const assert = require('assert');
const MultipartETag = require('./multipart_etag');
const fd_slicer = require('fd-slicer');
const mime = require('mime');
const StreamSink = require('streamsink');
const PassThrough = require('stream').PassThrough;

const MAX_PUTOBJECT_SIZE = 5 * 1024 * 1024 * 1024;
const MAX_DELETE_COUNT = 1000;
const MAX_MULTIPART_COUNT = 10000;
const MIN_MULTIPART_SIZE = 5 * 1024 * 1024;

const TO_UNIX_RE = new RegExp(quotemeta(path.sep), 'g');

exports.createClient = function(options) {
    return new Client(options);
};

exports.getPublicUrl = getPublicUrl;
exports.getPublicUrlHttp = getPublicUrlHttp;

exports.Client = Client;
exports.MultipartETag = MultipartETag;
exports.AWS = AWS;

exports.MAX_PUTOBJECT_SIZE = MAX_PUTOBJECT_SIZE;
exports.MAX_DELETE_COUNT = MAX_DELETE_COUNT;
exports.MAX_MULTIPART_COUNT = MAX_MULTIPART_COUNT;
exports.MIN_MULTIPART_SIZE = MIN_MULTIPART_SIZE;

function Client(options) {
    options = options || {};
    this.s3 = options.s3Client || new AWS.S3(options.s3Options);
    this.s3Pend = new Pend();
    this.s3Pend.max = options.maxAsyncS3 || 20;
    this.s3RetryCount = options.s3RetryCount || 3;
    this.s3RetryDelay = options.s3RetryDelay || 1000;
    this.multipartUploadThreshold = options.multipartUploadThreshold || 20 * 1024 * 1024;
    this.multipartUploadSize = options.multipartUploadSize || 15 * 1024 * 1024;
    this.multipartDownloadThreshold = options.multipartDownloadThreshold || 20 * 1024 * 1024;
    this.multipartDownloadSize = options.multipartDownloadSize || 15 * 1024 * 1024;

    if (this.multipartUploadThreshold < MIN_MULTIPART_SIZE) {
        throw new Error('Minimum multipartUploadThreshold is 5MB.');
    }
    if (this.multipartUploadThreshold > MAX_PUTOBJECT_SIZE) {
        throw new Error('Maximum multipartUploadThreshold is 5GB.');
    }
    if (this.multipartUploadSize < MIN_MULTIPART_SIZE) {
        throw new Error('Minimum multipartUploadSize is 5MB.');
    }
    if (this.multipartUploadSize > MAX_PUTOBJECT_SIZE) {
        throw new Error('Maximum multipartUploadSize is 5GB.');
    }
}

Client.prototype.deleteObjects = function(s3Params) {
    const self = this;
    const ee = new EventEmitter();
    const params = {
        Bucket: s3Params.Bucket,
        Delete: extend({}, s3Params.Delete),
        MFA: s3Params.MFA
    };
    const slices = chunkArray(params.Delete.Objects, MAX_DELETE_COUNT);
    const pend = new Pend();

    let errorOccurred = false;

    ee.progressAmount = 0;
    ee.progressTotal = params.Delete.Objects.length;

    slices.forEach(uploadSlice);
    pend.wait(function(err) {
        if (err) {
            ee.emit('error', err);
            return;
        }
        ee.emit('end');
    });
    return ee;

    function uploadSlice(slice) {
        pend.go(function(cb) {
            doWithRetry(tryDeletingObjects, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
                if (err) {
                    cb(err);
                } else {
                    ee.progressAmount += slice.length;
                    ee.emit('progress');
                    ee.emit('data', data);
                    cb();
                }
            });
        });

        function tryDeletingObjects(cb) {
            self.s3Pend.go(function(pendCb) {
                params.Delete.Objects = slice;
                self.s3.deleteObjects(params, function(err, data) {
                    pendCb();
                    cb(err, data);
                });
            });
        }
    }
};

Client.prototype.uploadFile = function(params) {
    const self = this;
    const uploaderEE = new EventEmitter();
    uploaderEE.progressMd5Amount = 0;
    uploaderEE.progressAmount = 0;
    uploaderEE.progressTotal = 0;
    uploaderEE.abort = handleAbort;
    uploaderEE.getPublicUrl = function() {
        return getPublicUrl(s3Params.Bucket, s3Params.Key, self.s3.config.region, self.s3.config.endpoint);
    };
    uploaderEE.getPublicUrlHttp = function() {
        return getPublicUrlHttp(s3Params.Bucket, s3Params.Key, self.s3.config.endpoint);
    };

    uploaderEE.localFile = params.localFile;
    let localFileStat = null;
    let s3Params = extend({}, params.s3Params);
    if (s3Params.ContentType === undefined) {
        let defaultContentType = params.defaultContentType || 'application/octet-stream';
        s3Params.ContentType = mime.getType(uploaderEE.localFile) || defaultContentType;
    }
    let fatalError = false;
    let localFileSlicer = null;
    let parts = [];

    openFile();

    return uploaderEE;

    function handleError(err) {
        if (localFileSlicer) {
            localFileSlicer.unref();
            localFileSlicer = null;
        }
        if (fatalError) return;
        fatalError = true;
        uploaderEE.emit('error', err);
    }

    function handleAbort() {
        fatalError = true;
    }

    function openFile() {
        fs.open(uploaderEE.localFile, 'r', function(err, fd) {
            if (err) return handleError(err);
            localFileSlicer = fd_slicer.createFromFd(fd, { autoClose: true });
            localFileSlicer.on('error', handleError);
            localFileSlicer.on('close', function() {
                uploaderEE.emit('fileClosed');
            });

            // keep an extra reference alive until we decide that we're completely
            // done with the file
            localFileSlicer.ref();

            uploaderEE.emit('fileOpened', localFileSlicer);

            fs.fstat(fd, function(err, stat) {
                if (err) return handleError(err);
                localFileStat = stat;
                uploaderEE.progressTotal = stat.size;
                startPuttingObject();
            });
        });
    }

    function startPuttingObject() {
        if (localFileStat.size >= self.multipartUploadThreshold) {
            let multipartUploadSize = self.multipartUploadSize;
            const partsRequiredCount = Math.ceil(localFileStat.size / multipartUploadSize);
            if (partsRequiredCount > MAX_MULTIPART_COUNT) {
                multipartUploadSize = smallestPartSizeFromFileSize(localFileStat.size);
            }
            if (multipartUploadSize > MAX_PUTOBJECT_SIZE) {
                let err = new Error('File size exceeds maximum object size: ' + uploaderEE.localFile);
                err.retryable = false;
                handleError(err);
                return;
            }
            startMultipartUpload(multipartUploadSize);
        } else {
            doWithRetry(tryPuttingObject, self.s3RetryCount, self.s3RetryDelay, onPutObjectDone);
        }

        function onPutObjectDone(err, data) {
            if (fatalError) return;
            if (err) return handleError(err);
            if (localFileSlicer) {
                localFileSlicer.unref();
                localFileSlicer = null;
            }
            uploaderEE.emit('end', data);
        }
    }

    function startMultipartUpload(multipartUploadSize) {
        doWithRetry(tryCreateMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
            if (fatalError) return;
            if (err) return handleError(err);
            uploaderEE.emit('data', data);
            s3Params = {
                Bucket: s3Params.Bucket,
                Key: encodeSpecialCharacters(s3Params.Key),
                SSECustomerAlgorithm: s3Params.SSECustomerAlgorithm,
                SSECustomerKey: s3Params.SSECustomerKey,
                SSECustomerKeyMD5: s3Params.SSECustomerKeyMD5
            };
            queueAllParts(data.UploadId, multipartUploadSize);
        });
    }

    function queueAllParts(uploadId, multipartUploadSize) {
        let cursor = 0;
        let nextPartNumber = 1;
        const pend = new Pend();
        while (cursor < localFileStat.size) {
            const start = cursor;
            let end = cursor + multipartUploadSize;
            if (end > localFileStat.size) {
                end = localFileStat.size;
            }
            cursor = end;
            const part = {
                ETag: null,
                PartNumber: nextPartNumber++
            };
            parts.push(part);
            pend.go(makeUploadPartFn(start, end, part, uploadId));
        }
        pend.wait(function(err) {
            if (fatalError) return;
            if (err) return handleError(err);
            completeMultipartUpload();
        });
    }

    function makeUploadPartFn(start, end, part, uploadId) {
        return function(cb) {
            doWithRetry(tryUploadPart, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
                if (fatalError) return;
                if (err) return handleError(err);
                uploaderEE.emit('part', data);
                cb();
            });
        };

        function tryUploadPart(cb) {
            if (fatalError) return;
            self.s3Pend.go(function(pendCb) {
                if (fatalError) {
                    pendCb();
                    return;
                }
                const inStream = localFileSlicer.createReadStream({ start: start, end: end });

                let errorOccurred = false;

                inStream.on('error', function(err) {
                    if (fatalError || errorOccurred) return;
                    handleError(err);
                });
                s3Params.ContentLength = end - start;
                s3Params.PartNumber = part.PartNumber;
                s3Params.UploadId = uploadId;

                const multipartETag = new MultipartETag({ size: s3Params.ContentLength, count: 1 });
                const pend = new Pend();

                let prevBytes = 0;
                let overallDelta = 0;
                let haveETag = pend.hold();
                multipartETag.on('progress', function() {
                    if (fatalError || errorOccurred) return;
                    let delta = multipartETag.bytes - prevBytes;
                    prevBytes = multipartETag.bytes;
                    uploaderEE.progressAmount += delta;
                    overallDelta += delta;
                    uploaderEE.emit('progress');
                });
                multipartETag.on('end', function() {
                    if (fatalError || errorOccurred) return;
                    let delta = multipartETag.bytes - prevBytes;
                    uploaderEE.progressAmount += delta;
                    uploaderEE.progressTotal += end - start - multipartETag.bytes;
                    uploaderEE.emit('progress');
                    haveETag();
                });
                inStream.pipe(multipartETag);
                s3Params.Body = multipartETag;

                self.s3.uploadPart(extend({}, s3Params), function(err, data) {
                    pendCb();
                    if (fatalError || errorOccurred) return;
                    if (err) {
                        errorOccurred = true;
                        uploaderEE.progressAmount -= overallDelta;
                        cb(err);
                        return;
                    }
                    pend.wait(function() {
                        if (fatalError) return;
                        if (!compareMultipartETag(data.ETag, multipartETag)) {
                            errorOccurred = true;
                            uploaderEE.progressAmount -= overallDelta;
                            cb(new Error('ETag does not match MD5 checksum'));
                            return;
                        }
                        part.ETag = data.ETag;
                        cb(null, data);
                    });
                });
            });
        }
    }

    function completeMultipartUpload() {
        localFileSlicer.unref();
        localFileSlicer = null;
        doWithRetry(tryCompleteMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
            if (fatalError) return;
            if (err) return handleError(err);
            uploaderEE.emit('end', data);
        });
    }

    function tryCompleteMultipartUpload(cb) {
        if (fatalError) return;
        self.s3Pend.go(function(pendCb) {
            if (fatalError) {
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
            self.s3.completeMultipartUpload(s3Params, function(err, data) {
                pendCb();
                if (fatalError) return;
                cb(err, data);
            });
        });
    }

    function tryCreateMultipartUpload(cb) {
        if (fatalError) return;
        self.s3Pend.go(function(pendCb) {
            if (fatalError) return pendCb();
            self.s3.createMultipartUpload(s3Params, function(err, data) {
                pendCb();
                if (fatalError) return;
                cb(err, data);
            });
        });
    }

    function tryPuttingObject(cb) {
        self.s3Pend.go(function(pendCb) {
            if (fatalError) return pendCb();
            const inStream = localFileSlicer.createReadStream();
            inStream.on('error', handleError);
            const pend = new Pend();
            const multipartETag = new MultipartETag({ size: localFileStat.size, count: 1 });
            pend.go(function(cb) {
                multipartETag.on('end', function() {
                    if (fatalError) return;
                    uploaderEE.progressAmount = multipartETag.bytes;
                    uploaderEE.progressTotal = multipartETag.bytes;
                    uploaderEE.emit('progress');
                    localFileStat.size = multipartETag.bytes;
                    localFileStat.multipartETag = multipartETag;
                    cb();
                });
            });
            multipartETag.on('progress', function() {
                if (fatalError) return;
                uploaderEE.progressAmount = multipartETag.bytes;
                uploaderEE.emit('progress');
            });
            s3Params.ContentLength = localFileStat.size;
            uploaderEE.progressAmount = 0;
            inStream.pipe(multipartETag);
            s3Params.Body = multipartETag;

            self.s3.putObject(s3Params, function(err, data) {
                pendCb();
                if (fatalError) return;
                if (err) {
                    cb(err);
                    return;
                }
                pend.wait(function() {
                    if (fatalError) return;
                    if (!compareMultipartETag(data.ETag, localFileStat.multipartETag)) {
                        cb(new Error('ETag does not match MD5 checksum'));
                        return;
                    }
                    cb(null, data);
                });
            });
        });
    }
};

Client.prototype.downloadFile = function(params) {
    const self = this;
    const downloaderEE = new EventEmitter();
    downloaderEE.localFile = params.localFile;
    const s3Params = extend({}, params.s3Params);

    const dirPath = path.dirname(downloaderEE.localFile);
    downloaderEE.progressAmount = 0;
    mkdirp(dirPath, function(err) {
        if (err) {
            downloaderEE.emit('error', err);
            return;
        }

        doWithRetry(doDownloadWithPend, self.s3RetryCount, self.s3RetryDelay, function(err) {
            if (err) {
                downloaderEE.emit('error', err);
                return;
            }
            downloaderEE.emit('end');
        });
    });

    return downloaderEE;

    function doDownloadWithPend(cb) {
        self.s3Pend.go(function(pendCb) {
            doTheDownload(function(err) {
                pendCb();
                cb(err);
            });
        });
    }

    function doTheDownload(cb) {
        const request = self.s3.getObject(s3Params);
        const hashCheckPend = new Pend();

        let errorOccurred = false;

        request.on('httpHeaders', function(statusCode, headers, resp) {
            if (statusCode >= 300) {
                handleError(new Error('http status code ' + statusCode));
                return;
            }
            if (headers['content-length'] == undefined) {
                const outStream = fs.createWriteStream(downloaderEE.localFile);
                outStream.on('error', handleError);
                downloaderEE.progressTotal = 0;
                downloaderEE.progressAmount = -1;
                request.on('httpData', function(chunk) {
                    downloaderEE.progressTotal += chunk.length;
                    downloaderEE.progressAmount += chunk.length;
                    downloaderEE.emit('progress');
                    outStream.write(chunk);
                });

                request.on('httpDone', function() {
                    if (errorOccurred) return;
                    downloaderEE.progressAmount += 1;
                    downloaderEE.emit('progress');
                    outStream.end();
                    cb();
                });
            } else {
                const contentLength = parseInt(headers['content-length'], 10);
                downloaderEE.progressTotal = contentLength;
                downloaderEE.progressAmount = 0;
                downloaderEE.emit('progress');
                downloaderEE.emit('httpHeaders', statusCode, headers, resp);
                const eTag = cleanETag(headers.etag);
                const eTagCount = getETagCount(eTag);

                const outStream = fs.createWriteStream(downloaderEE.localFile);
                const multipartETag = new MultipartETag({ size: contentLength, count: eTagCount });
                const httpStream = resp.httpResponse.createUnbufferedStream();

                httpStream.on('error', handleError);
                outStream.on('error', handleError);

                hashCheckPend.go(function(cb) {
                    multipartETag.on('end', function() {
                        if (multipartETag.bytes !== contentLength) {
                            handleError(new Error('Downloaded size does not match Content-Length'));
                            return;
                        }
                        if (eTagCount === 1 && !multipartETag.anyMatch(eTag)) {
                            handleError(new Error('ETag does not match MD5 checksum'));
                            return;
                        }
                        cb();
                    });
                });
                multipartETag.on('progress', function() {
                    downloaderEE.progressAmount = multipartETag.bytes;
                    downloaderEE.emit('progress');
                });
                outStream.on('close', function() {
                    if (errorOccurred) return;
                    hashCheckPend.wait(cb);
                });

                httpStream.pipe(multipartETag);
                httpStream.pipe(outStream);
                multipartETag.resume();
            }
        });

        request.send(handleError);

        function handleError(err) {
            if (!err) return;
            if (errorOccurred) return;
            errorOccurred = true;
            cb(err);
        }
    }
};

/* params:
 *  - recursive: false
 *  - s3Params:
 *    - Bucket: params.s3Params.Bucket,
 *    - Delimiter: null,
 *    - Marker: null,
 *    - MaxKeys: null,
 *    - Prefix: prefix,
 */
Client.prototype.listObjects = function(params) {
    const self = this;
    const ee = new EventEmitter();
    const s3Details = extend({}, params.s3Params);
    const recursive = !!params.recursive;

    let abort = false;

    ee.progressAmount = 0;
    ee.objectsFound = 0;
    ee.dirsFound = 0;
    findAllS3Objects(s3Details.Marker, s3Details.Prefix, function(err, data) {
        if (err) {
            ee.emit('error', err);
            return;
        }
        ee.emit('end');
    });

    ee.abort = function() {
        abort = true;
    };

    return ee;

    function findAllS3Objects(marker, prefix, cb) {
        if (abort) return;
        doWithRetry(listObjects, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
            if (abort) return;
            if (err) return cb(err);

            ee.progressAmount += 1;
            ee.objectsFound += data.Contents.length;
            ee.dirsFound += data.CommonPrefixes.length;
            ee.emit('progress');
            ee.emit('data', data);

            const pend = new Pend();

            if (recursive) {
                data.CommonPrefixes.forEach(recurse);
                data.CommonPrefixes = [];
            }

            if (data.IsTruncated) {
                pend.go(findNext1000);
            }

            pend.wait(function(err) {
                cb(err);
            });

            function findNext1000(cb) {
                const nextMarker = data.NextMarker || data.Contents[data.Contents.length - 1].Key;
                findAllS3Objects(nextMarker, prefix, cb);
            }

            function recurse(dirObj) {
                const prefix = dirObj.Prefix;
                pend.go(function(cb) {
                    findAllS3Objects(null, prefix, cb);
                });
            }
        });

        function listObjects(cb) {
            if (abort) return;
            self.s3Pend.go(function(pendCb) {
                if (abort) {
                    pendCb();
                    return;
                }
                s3Details.Marker = marker;
                s3Details.Prefix = prefix;
                self.s3.listObjects(s3Details, function(err, data) {
                    pendCb();
                    if (abort) return;
                    cb(err, data);
                });
            });
        }
    }
};

/* params:
 * - deleteRemoved - delete s3 objects with no corresponding local file. default false
 * - localDir - path on local file system to sync
 * - s3Params:
 *   - Bucket (required)
 *   - Key (required)
 */
Client.prototype.uploadDir = function(params) {
    return syncDir(this, params, true);
};

Client.prototype.downloadDir = function(params) {
    return syncDir(this, params, false);
};

Client.prototype.deleteDir = function(s3Params) {
    const self = this;
    const ee = new EventEmitter();
    const bucket = s3Params.Bucket;
    const mfa = s3Params.MFA;
    const listObjectsParams = {
        recursive: true,
        s3Params: {
            Bucket: bucket,
            Prefix: s3Params.Prefix
        }
    };
    const finder = self.listObjects(listObjectsParams);
    const pend = new Pend();
    ee.progressAmount = 0;
    ee.progressTotal = 0;
    finder.on('error', function(err) {
        ee.emit('error', err);
    });
    finder.on('data', function(objects) {
        ee.progressTotal += objects.Contents.length;
        ee.emit('progress');
        if (objects.Contents.length > 0) {
            pend.go(deleteThem);
        }

        function deleteThem(cb) {
            const params = {
                Bucket: bucket,
                Delete: {
                    Objects: objects.Contents.map(keyOnly),
                    Quiet: true
                },
                MFA: mfa
            };
            const deleter = self.deleteObjects(params);
            deleter.on('error', function(err) {
                finder.abort();
                ee.emit('error', err);
            });
            deleter.on('end', function() {
                ee.progressAmount += objects.Contents.length;
                ee.emit('progress');
                cb();
            });
        }
    });
    finder.on('end', function() {
        pend.wait(function() {
            ee.emit('end');
        });
    });
    return ee;
};

Client.prototype.copyObject = function(_s3Params) {
    const self = this;
    const ee = new EventEmitter();
    const s3Params = extend({}, _s3Params);
    delete s3Params.MFA;
    doWithRetry(doCopyWithPend, self.s3RetryCount, self.s3RetryDelay, function(err, data) {
        if (err) {
            ee.emit('error', err);
        } else {
            ee.emit('end', data);
        }
    });
    function doCopyWithPend(cb) {
        self.s3Pend.go(function(pendCb) {
            doTheCopy(function(err, data) {
                pendCb();
                cb(err, data);
            });
        });
    }
    function doTheCopy(cb) {
        self.s3.copyObject(s3Params, cb);
    }
    return ee;
};

Client.prototype.moveObject = function(s3Params) {
    const self = this;
    const ee = new EventEmitter();
    const copier = self.copyObject(s3Params);
    const copySource = s3Params.CopySource;
    const mfa = s3Params.MFA;
    copier.on('error', function(err) {
        ee.emit('error', err);
    });
    copier.on('end', function(data) {
        ee.emit('copySuccess', data);
        const slashIndex = copySource.indexOf('/');
        const sourceBucket = copySource.substring(0, slashIndex);
        const sourceKey = copySource.substring(slashIndex + 1);
        const deleteS3Params = {
            Bucket: sourceBucket,
            Delete: {
                Objects: [
                    {
                        Key: sourceKey
                    }
                ],
                Quiet: true
            },
            MFA: mfa
        };
        const deleter = self.deleteObjects(deleteS3Params);
        deleter.on('error', function(err) {
            ee.emit('error', err);
        });
        let deleteData;
        deleter.on('data', function(data) {
            deleteData = data;
        });
        deleter.on('end', function() {
            ee.emit('end', deleteData);
        });
    });
    return ee;
};

Client.prototype.downloadBuffer = function(s3Params) {
    const self = this;
    const downloaderEE = new EventEmitter();
    s3Params = extend({}, s3Params);

    downloaderEE.progressAmount = 0;

    doWithRetry(doDownloadWithPend, self.s3RetryCount, self.s3RetryDelay, function(err, buffer) {
        if (err) {
            downloaderEE.emit('error', err);
            return;
        }
        downloaderEE.emit('end', buffer);
    });

    return downloaderEE;

    function doDownloadWithPend(cb) {
        self.s3Pend.go(function(pendCb) {
            doTheDownload(function(err, buffer) {
                pendCb();
                cb(err, buffer);
            });
        });
    }

    function doTheDownload(cb) {
        const errorOccurred = false;
        const request = self.s3.getObject(s3Params);
        const hashCheckPend = new Pend();
        request.on('httpHeaders', function(statusCode, headers, resp) {
            if (statusCode >= 300) {
                handleError(new Error('http status code ' + statusCode));
                return;
            }
            const contentLength = parseInt(headers['content-length'], 10);
            downloaderEE.progressTotal = contentLength;
            downloaderEE.progressAmount = 0;
            downloaderEE.emit('progress');
            downloaderEE.emit('httpHeaders', statusCode, headers, resp);
            const eTag = cleanETag(headers.etag);
            const eTagCount = getETagCount(eTag);

            const outStream = new StreamSink();
            const multipartETag = new MultipartETag({ size: contentLength, count: eTagCount });
            const httpStream = resp.httpResponse.createUnbufferedStream();

            httpStream.on('error', handleError);
            outStream.on('error', handleError);

            hashCheckPend.go(function(cb) {
                multipartETag.on('end', function() {
                    if (multipartETag.bytes !== contentLength) {
                        handleError(new Error('Downloaded size does not match Content-Length'));
                        return;
                    }
                    if (eTagCount === 1 && !multipartETag.anyMatch(eTag)) {
                        handleError(new Error('ETag does not match MD5 checksum'));
                        return;
                    }
                    cb();
                });
            });
            multipartETag.on('progress', function() {
                downloaderEE.progressAmount = multipartETag.bytes;
                downloaderEE.emit('progress');
            });
            outStream.on('finish', function() {
                if (errorOccurred) return;
                hashCheckPend.wait(function() {
                    cb(null, outStream.toBuffer());
                });
            });

            httpStream.pipe(multipartETag);
            httpStream.pipe(outStream);
            multipartETag.resume();
        });

        request.send(handleError);

        function handleError(err) {
            if (!err) return;
            if (errorOccurred) return;
            errorOccurred = true;
            cb(err);
        }
    }
};

Client.prototype.downloadStream = function(s3Params) {
    const self = this;
    const downloadStream = new PassThrough();
    s3Params = extend({}, s3Params);

    doDownloadWithPend(function(err) {
        if (err) downloadStream.emit('error', err);
    });
    return downloadStream;

    function doDownloadWithPend(cb) {
        self.s3Pend.go(function(pendCb) {
            doTheDownload(function(err) {
                pendCb();
                cb(err);
            });
        });
    }

    function doTheDownload(cb) {
        const request = self.s3.getObject(s3Params);
        const hashCheckPend = new Pend();

        let errorOccurred = false;

        request.on('httpHeaders', function(statusCode, headers, resp) {
            if (statusCode >= 300) {
                handleError(new Error('http status code ' + statusCode));
                return;
            }
            downloadStream.emit('httpHeaders', statusCode, headers, resp);
            const httpStream = resp.httpResponse.createUnbufferedStream();

            httpStream.on('error', handleError);

            downloadStream.on('finish', function() {
                if (errorOccurred) return;
                cb();
            });

            httpStream.pipe(downloadStream);
        });

        request.send(handleError);

        function handleError(err) {
            if (!err) return;
            if (errorOccurred) return;
            errorOccurred = true;
            cb(err);
        }
    }
};

function syncDir(self, params, directionIsToS3) {
    const ee = new EventEmitter();
    const finditOpts = {
        fs: fs,
        followSymlinks: params.followSymlinks == null ? true : !!params.followSymlinks
    };
    ee.localDir = params.localDir;
    const deleteRemoved = params.deleteRemoved === true;
    let fatalError = false;
    const prefix = params.s3Params.Prefix ? ensureSlash(params.s3Params.Prefix) : '';
    const bucket = params.s3Params.Bucket;
    const listObjectsParams = {
        recursive: true,
        s3Params: {
            Bucket: bucket,
            Marker: null,
            MaxKeys: null,
            Prefix: prefix
        }
    };
    const getS3Params = params.getS3Params;
    const baseUpDownS3Params = extend({}, params.s3Params);
    const upDownFileParams = {
        localFile: null,
        s3Params: baseUpDownS3Params,
        defaultContentType: params.defaultContentType
    };
    delete upDownFileParams.s3Params.Prefix;

    ee.activeTransfers = 0;
    ee.progressAmount = 0;
    ee.progressTotal = 0;
    ee.progressMd5Amount = 0;
    ee.progressMd5Total = 0;
    ee.objectsFound = 0;
    ee.filesFound = 0;
    ee.deleteAmount = 0;
    ee.deleteTotal = 0;
    ee.doneFindingFiles = false;
    ee.doneFindingObjects = false;
    ee.doneMd5 = false;

    let allLocalFiles = [];
    let allS3Objects = [];
    let localFileCursor = 0;
    let s3ObjectCursor = 0;
    let objectsToDelete = [];

    findAllS3Objects();
    startFindAllFiles();

    return ee;

    function flushDeletes() {
        if (objectsToDelete.length === 0) return;
        const thisObjectsToDelete = objectsToDelete;
        objectsToDelete = [];
        const params = {
            Bucket: bucket,
            Delete: {
                Objects: thisObjectsToDelete,
                Quiet: true
            }
        };
        const deleter = self.deleteObjects(params);
        deleter.on('error', handleError);
        deleter.on('end', function() {
            if (fatalError) return;
            ee.deleteAmount += thisObjectsToDelete.length;
            ee.emit('progress');
            checkDoMoreWork();
        });
    }

    function checkDoMoreWork() {
        if (fatalError) return;

        const localFileStat = allLocalFiles[localFileCursor];
        const s3Object = allS3Objects[s3ObjectCursor];

        // need to wait for a file or object. checkDoMoreWork will get called
        // again when that happens.
        if (!localFileStat && !ee.doneMd5) return;
        if (!s3Object && !ee.doneFindingObjects) return;

        // need to wait until the md5 is done computing for the local file
        if (localFileStat && !localFileStat.multipartETag) return;

        // localFileStat or s3Object could still be null - in that case we have
        // reached the real end of the list.

        // if they're both null, we've reached the true end
        if (!localFileStat && !s3Object) {
            // if we don't have any pending deletes or uploads, we're actually done
            flushDeletes();
            if (ee.deleteAmount >= ee.deleteTotal && ee.progressAmount >= ee.progressTotal && ee.activeTransfers === 0) {
                ee.emit('end');
                // prevent checkDoMoreWork from doing any more work
                fatalError = true;
            }
            // either way, there's nothing else to do in this method
            return;
        }

        // special case for directories when deleteRemoved is true and we're
        // downloading a dir from S3. We don't add directories to the list
        // unless this case is true, so we assert that fact here.
        if (localFileStat && localFileStat.isDirectory()) {
            assert.ok(!directionIsToS3);
            assert.ok(deleteRemoved);

            localFileCursor += 1;
            setImmediate(checkDoMoreWork);

            if (!s3Object || s3Object.key.indexOf(localFileStat.s3Path) !== 0) {
                deleteLocalDir();
            }
            return;
        }

        if (directionIsToS3) {
            if (!localFileStat) {
                deleteS3Object();
            } else if (!s3Object) {
                uploadLocalFile();
            } else if (localFileStat.s3Path < s3Object.key) {
                uploadLocalFile();
            } else if (localFileStat.s3Path > s3Object.key) {
                deleteS3Object();
            } else if (!compareMultipartETag(s3Object.ETag, localFileStat.multipartETag)) {
                // both file cursor and s3 cursor should increment
                s3ObjectCursor += 1;
                uploadLocalFile();
            } else {
                skipThisOne();
            }
        } else {
            if (!localFileStat) {
                downloadS3Object();
            } else if (!s3Object) {
                deleteLocalFile();
            } else if (localFileStat.s3Path < s3Object.key) {
                deleteLocalFile();
            } else if (localFileStat.s3Path > s3Object.key) {
                downloadS3Object();
            } else if (!compareMultipartETag(s3Object.ETag, localFileStat.multipartETag)) {
                // both file cursor and s3 cursor should increment
                localFileCursor += 1;
                downloadS3Object();
            } else {
                skipThisOne();
            }
        }

        function deleteLocalDir() {
            const fullPath = path.join(ee.localDir, localFileStat.path);
            ee.deleteTotal += 1;
            rimraf(fullPath, function(err) {
                if (fatalError) return;
                if (err && err.code !== 'ENOENT') return handleError(err);
                ee.deleteAmount += 1;
                ee.emit('progress');
                checkDoMoreWork();
            });
        }

        function deleteLocalFile() {
            localFileCursor += 1;
            setImmediate(checkDoMoreWork);
            if (!deleteRemoved) return;
            ee.deleteTotal += 1;
            const fullPath = path.join(ee.localDir, localFileStat.path);
            fs.unlink(fullPath, function(err) {
                if (fatalError) return;
                if (err && err.code !== 'ENOENT') return handleError(err);
                ee.deleteAmount += 1;
                ee.emit('progress');
                checkDoMoreWork();
            });
        }

        function downloadS3Object() {
            s3ObjectCursor += 1;
            setImmediate(checkDoMoreWork);
            const fullPath = path.join(ee.localDir, toNativeSep(s3Object.key));

            if (getS3Params) {
                getS3Params(fullPath, s3Object, haveS3Params);
            } else {
                startDownload();
            }

            function haveS3Params(err, s3Params) {
                if (fatalError) return;
                if (err) return handleError(err);

                if (!s3Params) {
                    // user has decided to skip this file
                    return;
                }

                upDownFileParams.s3Params = extend(extend({}, baseUpDownS3Params), s3Params);
                startDownload();
            }

            function startDownload() {
                ee.progressTotal += s3Object.Size;
                const fullKey = s3Object.Key;
                upDownFileParams.s3Params.Key = fullKey;
                upDownFileParams.localFile = fullPath;
                const downloaderEE = self.downloadFile(upDownFileParams);
                let prevAmountDone = 0;
                let delta;
                ee.activeTransfers++;
                ee.emit('fileDownloadStart', fullPath, fullKey);
                downloaderEE.on('error', handleError);
                downloaderEE.on('progress', function() {
                    if (fatalError) return;
                    delta = downloaderEE.progressAmount - prevAmountDone;
                    prevAmountDone = downloaderEE.progressAmount;
                    ee.progressAmount += delta;
                    ee.emit('progress');
                });
                downloaderEE.on('end', function() {
                    ee.activeTransfers--;
                    ee.emit('fileDownloadEnd', fullPath, fullKey);
                    ee.emit('progress');
                    checkDoMoreWork();
                });
            }
        }

        function skipThisOne() {
            s3ObjectCursor += 1;
            localFileCursor += 1;
            setImmediate(checkDoMoreWork);
        }

        function uploadLocalFile() {
            localFileCursor += 1;
            setImmediate(checkDoMoreWork);
            const fullPath = path.join(ee.localDir, localFileStat.path);

            if (getS3Params) {
                getS3Params(fullPath, localFileStat, haveS3Params);
            } else {
                upDownFileParams.s3Params = baseUpDownS3Params;
                startUpload();
            }

            function haveS3Params(err, s3Params) {
                if (fatalError) return;
                if (err) return handleError(err);

                if (!s3Params) {
                    // user has decided to skip this file
                    return;
                }

                upDownFileParams.s3Params = extend(extend({}, baseUpDownS3Params), s3Params);
                startUpload();
            }

            function startUpload() {
                ee.progressTotal += localFileStat.size;
                const fullKey = prefix + localFileStat.s3Path;
                upDownFileParams.s3Params.Key = fullKey;
                upDownFileParams.localFile = fullPath;
                const uploaderEE = self.uploadFile(upDownFileParams);
                let prevAmountDone = 0;
                let prevAmountTotal = localFileStat.size;
                let amountDelta;
                let totalDelta;
                ee.activeTransfers++;
                ee.emit('fileUploadStart', fullPath, fullKey);
                uploaderEE.on('error', handleError);
                uploaderEE.on('progress', function() {
                    if (fatalError) return;
                    amountDelta = uploaderEE.progressAmount - prevAmountDone;
                    prevAmountDone = uploaderEE.progressAmount;
                    ee.progressAmount += amountDelta;

                    totalDelta = uploaderEE.progressTotal - prevAmountTotal;
                    prevAmountTotal = uploaderEE.progressTotal;
                    ee.progressTotal += totalDelta;

                    ee.emit('progress');
                });
                uploaderEE.on('end', function() {
                    ee.activeTransfers--;
                    ee.emit('fileUploadEnd', fullPath, fullKey);
                    ee.emit('progress');
                    checkDoMoreWork();
                });
            }
        }

        function deleteS3Object() {
            s3ObjectCursor += 1;
            setImmediate(checkDoMoreWork);
            if (!deleteRemoved) return;
            objectsToDelete.push({ Key: s3Object.Key });
            ee.deleteTotal += 1;
            ee.emit('progress');
            assert.ok(objectsToDelete.length <= 1000);
            if (objectsToDelete.length === 1000) {
                flushDeletes();
            }
        }
    }

    function handleError(err) {
        if (fatalError) return;
        fatalError = true;
        ee.emit('error', err);
    }

    function findAllS3Objects() {
        const finder = self.listObjects(listObjectsParams);
        finder.on('error', handleError);
        finder.on('data', function(data) {
            if (fatalError) return;
            ee.objectsFound += data.Contents.length;
            ee.emit('progress');
            data.Contents.forEach(function(object) {
                object.key = object.Key.substring(prefix.length);
                allS3Objects.push(object);
            });
            checkDoMoreWork();
        });
        finder.on('end', function() {
            if (fatalError) return;
            ee.doneFindingObjects = true;
            ee.emit('progress');
            checkDoMoreWork();
        });
    }

    function startFindAllFiles() {
        findAllFiles(function(err) {
            if (fatalError) return;
            if (err) return handleError(err);

            ee.doneFindingFiles = true;
            ee.emit('progress');

            allLocalFiles.sort(function(a, b) {
                if (a.s3Path < b.s3Path) {
                    return -1;
                } else if (a.s3Path > b.s3Path) {
                    return 1;
                } else {
                    return 0;
                }
            });
            startComputingMd5Sums();
        });
    }

    function startComputingMd5Sums() {
        let index = 0;
        computeOne();

        function computeOne() {
            if (fatalError) return;
            const localFileStat = allLocalFiles[index];
            if (!localFileStat) {
                ee.doneMd5 = true;
                ee.emit('progress');
                checkDoMoreWork();
                return;
            }
            if (localFileStat.multipartETag) {
                index += 1;
                setImmediate(computeOne);
                return;
            }
            const fullPath = path.join(ee.localDir, localFileStat.path);
            const inStream = fs.createReadStream(fullPath);
            const multipartETag = new MultipartETag();
            inStream.on('error', handleError);
            let prevBytes = 0;
            let delta;
            multipartETag.on('progress', function() {
                delta = multipartETag.bytes - prevBytes;
                prevBytes = multipartETag.bytes;
                ee.progressMd5Amount += delta;
            });
            multipartETag.on('end', function() {
                if (fatalError) return;
                localFileStat.multipartETag = multipartETag;
                checkDoMoreWork();
                ee.emit('progress');
                index += 1;
                computeOne();
            });
            inStream.pipe(multipartETag);
            multipartETag.resume();
        }
    }

    function findAllFiles(cb) {
        const dirWithSlash = ensureSep(ee.localDir);
        const walker = findit(dirWithSlash, finditOpts);
        walker.on('error', function(err) {
            walker.stop();
            // when uploading, we don't want to delete based on a nonexistent source directory
            // but when downloading, the destination directory does not have to exist.
            if (!directionIsToS3 && err.path === dirWithSlash && err.code === 'ENOENT') {
                cb();
            } else {
                cb(err);
            }
        });
        walker.on('directory', function(dir, stat, stop, linkPath) {
            if (fatalError) return walker.stop();
            // we only need to save directories when deleteRemoved is true
            // and we're syncing to disk from s3
            if (!deleteRemoved || directionIsToS3) return;
            const relPath = path.relative(ee.localDir, linkPath || dir);
            if (relPath === '') return;
            stat.path = relPath;
            stat.s3Path = toUnixSep(relPath) + '/';
            stat.multipartETag = new MultipartETag();
            allLocalFiles.push(stat);
        });
        walker.on('file', function(file, stat, linkPath) {
            if (fatalError) return walker.stop();
            const relPath = path.relative(ee.localDir, linkPath || file);
            stat.path = relPath;
            stat.s3Path = toUnixSep(relPath);
            ee.filesFound += 1;
            ee.progressMd5Total += stat.size;
            ee.emit('progress');
            allLocalFiles.push(stat);
        });
        walker.on('end', function() {
            cb();
        });
    }
}

function ensureChar(str, c) {
    return str[str.length - 1] === c ? str : str + c;
}

function ensureSep(dir) {
    return ensureChar(dir, path.sep);
}

function ensureSlash(dir) {
    return ensureChar(dir, '/');
}

function doWithRetry(fn, tryCount, delay, cb) {
    let tryIndex = 0;

    tryOnce();

    function tryOnce() {
        fn(function(err, result) {
            if (err) {
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

function extend(target, source) {
    for (const propName in source) {
        target[propName] = source[propName];
    }
    return target;
}

function chunkArray(array, maxLength) {
    let slices = [array];
    while (slices[slices.length - 1].length > maxLength) {
        slices.push(slices[slices.length - 1].splice(maxLength));
    }
    return slices;
}

function cleanETag(eTag) {
    return eTag ? eTag.replace(/^\s*'?\s*"?\s*(.*?)\s*"?\s*'?\s*$/, '$1') : '';
}

function compareMultipartETag(eTag, multipartETag) {
    return multipartETag.anyMatch(cleanETag(eTag));
}

function getETagCount(eTag) {
    let match = (eTag || '').match(/[a-fA-F0-9]{32}-(\d+)$/);
    return match ? parseInt(match[1], 10) : 1;
}

function keyOnly(item) {
    return {
        Key: item.Key,
        VersionId: item.VersionId
    };
}

function encodeSpecialCharacters(filename) {
    // Note: these characters are valid in URIs, but S3 does not like them for
    // some reason.
    return encodeURI(filename).replace(/[!'()* ]/g, function(char) {
        return '%' + char.charCodeAt(0).toString(16);
    });
}

function getPublicUrl(bucket, key, bucketLocation, endpoint) {
    const nonStandardBucketLocation = bucketLocation && bucketLocation !== 'us-east-1';
    const hostnamePrefix = nonStandardBucketLocation ? 's3-' + bucketLocation : 's3';
    const parts = {
        protocol: 'https:',
        hostname: hostnamePrefix + '.' + (endpoint || 'amazonaws.com'),
        pathname: '/' + bucket + '/' + encodeSpecialCharacters(key)
    };
    return url.format(parts);
}

function getPublicUrlHttp(bucket, key, endpoint) {
    const parts = {
        protocol: 'http:',
        hostname: bucket + '.' + (endpoint || 's3.amazonaws.com'),
        pathname: '/' + encodeSpecialCharacters(key)
    };
    return url.format(parts);
}

function toUnixSep(str) {
    return str.replace(TO_UNIX_RE, '/');
}

function toNativeSep(str) {
    return str.replace(/\//g, path.sep);
}

function quotemeta(str) {
    return String(str).replace(/(\W)/g, '\\$1');
}

function smallestPartSizeFromFileSize(fileSize) {
    const partSize = Math.ceil(fileSize / MAX_MULTIPART_COUNT);
    return partSize < MIN_MULTIPART_SIZE ? MIN_MULTIPART_SIZE : partSize;
}
