import { config } from 'dotenv';
const envResult = config();
if (envResult.error) throw envResult.error;
import { createClient as _createClient, getPublicUrl, getPublicUrlHttp } from '../lib/index.js';
import MultipartETag from '../lib/multipart_etag';
import { join, dirname } from 'path';
import ncp from 'ncp';
import Pend from 'pend';
import assert, { ok, strictEqual } from 'assert';
import { createWriteStream, createReadStream, writeFileSync, existsSync, unlink } from 'fs';
import mkdirp from 'mkdirp';
import { createHash } from 'crypto';
import rimraf from 'rimraf';
import StreamSink from 'streamsink';
const tempDir = join(__dirname, 'tmp');
const tempManyFilesDir = join(__dirname, 'tmp', 'many-files-dir');
const localFile = join(tempDir, 'random.png');
const remoteRoot = "node-s3-test/";
const remoteFile = remoteRoot + "file.png";
const remoteFile2 = remoteRoot + "file2.png";
const remoteFile3 = remoteRoot + "file3.png";
const remoteDir = remoteRoot + "dir1";
const remoteManyFilesDir = remoteRoot + "many-files-dir";

const describe = global.describe;
const it = global.it;
const after = global.after;
const before = global.before;

const s3Bucket = process.env.S3_BUCKET;

if (!s3Bucket || !process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
  console.log("S3_BUCKET, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY env lets needed to run tests");
  process.exit(1);
}

function createClient() {
  return _createClient({
    multipartUploadThreshold: 15 * 1024 * 1024,
    multipartUploadSize: 5 * 1024 * 1024,
    s3Options: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      endpoint: process.env.S3_ENDPOINT,
    },
  });
}

function createBigFile(file, size, cb) {
  mkdirp(dirname(file), function(err) {
    if (err) return cb(err);
    const md5sum = createHash('md5');
    const out = createWriteStream(file);
    out.on('error', function(err) {
      cb(err);
    });
    out.on('close', function() {
      cb(null, md5sum.digest('hex'));
    });
    const str = "abcdefghijklmnopqrstuvwxyz";
    let buf = "";
    for (let i = 0; i < size; ++i) {
      buf += str[i % str.length];
    }
    out.write(buf);
    md5sum.update(buf);
    out.end();
  });
}

function createFolderOfFiles(dir, numFiles, sizeOfFiles, cb) {
  for (let i = 0, j = numFiles; i < numFiles; i++) {
    createBigFile(join(dir, 'file' + i), sizeOfFiles, function () {
      j--;
      if (j === 0) {
        cb();
      }
    });
  }
}

const file1Md5 = "b1946ac92492d2347c6235b4d2611184";
describe("MultipartETag", function() {
  it("returns unmodified digest", function(done) {
    const inStream = createReadStream(join(__dirname, "dir1", "file1"));
    const multipartETag = new MultipartETag();
    let bytes;
    let progressEventCount = 0;
    multipartETag.on('progress', function() {
      bytes = multipartETag.bytes;
      progressEventCount += 1;
    });
    multipartETag.on('end', function() {
      ok(progressEventCount > 0);
      strictEqual(bytes, 6);
      strictEqual(multipartETag.digest.toString('hex'), file1Md5);
      ok(multipartETag.anyMatch(file1Md5));
      strictEqual(multipartETag.anyMatch(""), false);
      strictEqual(multipartETag.anyMatch(null), false);
      done();
    });
    inStream.pipe(multipartETag);
    multipartETag.resume();
  });
});

describe("s3", function () {
  let hexdigest;

  before(function(done) {
    const client = createClient();
    const s3Params = {
      Prefix: remoteRoot,
      Bucket: s3Bucket,
    };
    const deleter = client.deleteDir(s3Params);
    deleter.on('end', function() {
      done();
    });
  });

  after(function(done) {
    rimraf(tempDir, done);
  });

  after(function() {
    writeFileSync(join(__dirname, "dir3", "index.html"), "");
  });

  it("get public URL", function() {
    let httpsUrl = getPublicUrl("mybucket", "path/to/key");
    strictEqual(httpsUrl, "https://s3.amazonaws.com/mybucket/path/to/key");
    let httpUrl = getPublicUrlHttp("mybucket", "path/to/key");
    strictEqual(httpUrl, "http://mybucket.s3.amazonaws.com/path/to/key");
    // treat slashes literally
    httpsUrl = getPublicUrl("marina-restaurant.at", "uploads/about_restaurant_10.jpg", "eu-west-1");
    strictEqual(httpsUrl,
      "https://s3-eu-west-1.amazonaws.com/marina-restaurant.at/uploads/about_restaurant_10.jpg")
  });

  it("uploads", function(done) {
    createBigFile(localFile, 120 * 1024, function (err, _hexdigest) {
      if (err) return done(err);
      hexdigest = _hexdigest;
      let client = createClient();
      let params = {
        localFile: localFile,
        s3Params: {
          Key: remoteFile,
          Bucket: s3Bucket,
        },
      };
      let uploader = client.uploadFile(params);
      uploader.on('error', done);
      let progress = 0;
      let progressEventCount = 0;
      uploader.on('progress', function() {
        let amountDone = uploader.progressAmount;
        let amountTotal = uploader.progressTotal;
        let newProgress = amountDone / amountTotal;
        progressEventCount += 1;
        assert(newProgress >= progress, "old progress: " + progress + ", new progress: " + newProgress);
        progress = newProgress;
      });
      uploader.on('end', function(url) {
        strictEqual(progress, 1);
        assert(progressEventCount >= 2, "expected at least 2 progress events. got " + progressEventCount);
        assert(url !== "", "expected a url. got " + url);
        done();
      });
    });
  });

  it("downloads", function(done) {
    doDownloadFileTest(done);
  });

  it("downloadBuffer", function(done) {
    let client = createClient();
    let downloader = client.downloadBuffer({Key: remoteFile, Bucket: s3Bucket});
    downloader.on('error', done);
    let progress = 0;
    let progressEventCount = 0;
    let gotHttpHeaders = false;
    downloader.on('progress', function() {
      let amountDone = downloader.progressAmount;
      let amountTotal = downloader.progressTotal;
      let newProgress = amountDone / amountTotal;
      progressEventCount += 1;
      assert(newProgress >= progress, "old progress: " + progress + ", new progress: " + newProgress);
      progress = newProgress;
    });
    downloader.on('httpHeaders', function(statusCode, headers, resp) {
      let contentType = headers['content-type'];
      strictEqual(contentType, "image/png");
      gotHttpHeaders = true;
    });
    downloader.on('end', function(buffer) {
      strictEqual(progress, 1);
      assert(progressEventCount >= 3, "expected at least 3 progress events. got " + progressEventCount);
      let md5sum = createHash('md5');
      md5sum.update(buffer);
      strictEqual(md5sum.digest('hex'), hexdigest)
      ok(gotHttpHeaders);
      done();
    });
  });

  it("downloadStream", function(done) {
    let client = createClient();
    let downloadStream = client.downloadStream({Key: remoteFile, Bucket: s3Bucket});
    downloadStream.on('error', done);
    let gotHttpHeaders = false;
    downloadStream.on('httpHeaders', function(statusCode, headers, resp) {
      let contentType = headers['content-type'];
      strictEqual(contentType, "image/png");
      gotHttpHeaders = true;
    });
    let sink = new StreamSink();
    downloadStream.pipe(sink);
    sink.on('finish', function() {
      let md5sum = createHash('md5');
      md5sum.update(sink.toBuffer());
      strictEqual(md5sum.digest('hex'), hexdigest)
      ok(gotHttpHeaders);
      done();
    });
  });

  it("lists objects", function(done) {
    let params = {
      recursive: true,
      s3Params: {
        Bucket: s3Bucket,
        Prefix: remoteRoot,
      },
    };
    let client = createClient();
    let finder = client.listObjects(params);
    let found = false;
    finder.on('data', function(data) {
      strictEqual(data.Contents.length, 1);
      found = true;
    });
    finder.on('end', function() {
      strictEqual(found, true);
      done();
    });
  });

  it("copies an object", function(done) {
    let s3Params = {
      Bucket: s3Bucket,
      CopySource: s3Bucket + '/' + remoteFile,
      Key: remoteFile2,
    };
    let client = createClient();
    let copier = client.copyObject(s3Params);
    copier.on('end', function(data) {
      done();
    });
  });

  it("moves an object", function(done) {
    let s3Params = {
      Bucket: s3Bucket,
      CopySource: s3Bucket + '/' + remoteFile2,
      Key: remoteFile3,
    };
    let client = createClient();
    let copier = client.moveObject(s3Params);
    copier.on('end', function(data) {
      done();
    });
  });

  it("deletes an object", function(done) {
      let client = createClient();
      let params = {
        Bucket: s3Bucket,
        Delete: {
          Objects: [
            {
              Key: remoteFile,
            },
            {
              Key: remoteFile3,
            },
          ],
        },
      };
      let deleter = client.deleteObjects(params);
      deleter.on('end', function() {
        done();
      });
  });

  it("uploads a folder", function(done) {
    let client = createClient();
    let params = {
      localDir: join(__dirname, "dir1"),
      s3Params: {
        Prefix: remoteDir,
        Bucket: s3Bucket,
      },
    };
    let uploader = client.uploadDir(params);
    uploader.on('end', function() {
      done();
    });
  });

  it("downloads a folder", function(done) {
    let client = createClient();
    let localDir = join(tempDir, "dir-copy");
    let params = {
      localDir: localDir,
      s3Params: {
        Prefix: remoteDir,
        Bucket: s3Bucket,
      },
    };
    let downloader = client.downloadDir(params);
    downloader.on('end', function() {
      assertFilesMd5([
        {
          path: join(localDir, "file1"),
          md5: file1Md5,
        },
        {
          path: join(localDir, "file2"),
          md5: "6f0f1993fceae490cedfb1dee04985af",
        },
        {
          path: join(localDir, "inner1/a"),
          md5: "ebcb2061cab1d5c35241a79d27dce3af",
        },
        {
          path: join(localDir, "inner2/b"),
          md5: "c96b1cbe66f69b234cf361d8c1e5bbb9",
        },
      ], done);
    });
  });

  it("uploadDir with deleteRemoved", function(done) {
    let client = createClient();
    let params = {
      localDir: join(__dirname, "dir2"),
      deleteRemoved: true,
      s3Params: {
        Prefix: remoteDir,
        Bucket: s3Bucket,
      },
    };
    let uploader = client.uploadDir(params);
    uploader.on('end', function() {
      done();
    });
  });

  it("lists objects", function(done) {
    let params = {
      recursive: true,
      s3Params: {
        Bucket: s3Bucket,
        Prefix: remoteDir,
      },
    };
    let client = createClient();
    let finder = client.listObjects(params);
    let found = false;
    finder.on('data', function(data) {
      strictEqual(data.Contents.length, 2);
      strictEqual(data.CommonPrefixes.length, 0);
      found = true;
    });
    finder.on('end', function() {
      strictEqual(found, true);
      done();
    });
  });

  it("downloadDir with deleteRemoved", function(done) {
    let localDir = join(__dirname, "dir1");
    let localTmpDir = join(tempDir, "dir1");
    ncp(localDir, localTmpDir, function(err) {
      if (err) throw err;

      let client = createClient();
      let localDir = join(tempDir, "dir-copy");
      let params = {
        localDir: localTmpDir,
        deleteRemoved: true,
        s3Params: {
          Prefix: remoteDir,
          Bucket: s3Bucket,
        },
      };
      let downloader = client.downloadDir(params);
      downloader.on('end', function() {
        assertFilesMd5([
          {
            path: join(localTmpDir, "file1"),
            md5: "b1946ac92492d2347c6235b4d2611184",
          },
          {
            path: join(localTmpDir, "inner1/a"),
            md5: "ebcb2061cab1d5c35241a79d27dce3af",
          },
        ], function(err) {
          if (err) throw err;
          strictEqual(existsSync(join(localTmpDir, "file2")), false);
          strictEqual(existsSync(join(localTmpDir, "inner2/b")), false);
          strictEqual(existsSync(join(localTmpDir, "inner2")), false);
          done();
        });
      });
    });
  });

  it("upload folder with delete removed handles updates correctly", function(done) {
    let client = createClient();
    let params = {
      localDir: join(__dirname, "dir3"),
      deleteRemoved: true,
      s3Params: {
        Prefix: remoteDir,
        Bucket: s3Bucket,
      },
    };
    let uploader = client.uploadDir(params);
    uploader.on('end', function() {
      // modify a file and upload again. Make sure the list is still intact.
      writeFileSync(join(__dirname, "dir3", "index.html"), "hi");
      let uploader = client.uploadDir(params);
      uploader.on('end', function() {
        let params = {
          recursive: true,
          s3Params: {
            Bucket: s3Bucket,
            Prefix: remoteDir,
          },
        };
        let client = createClient();
        let finder = client.listObjects(params);
        let found = false;
        finder.on('data', function(data) {
          strictEqual(data.Contents.length, 2);
          strictEqual(data.CommonPrefixes.length, 0);
          found = true;
        });
        finder.on('end', function() {
          strictEqual(found, true);
          done();
        });
      });
    });
  });

  it("uploads folder with lots of files", function(done) {
    createFolderOfFiles(tempManyFilesDir, 10, 100 * 1024, function() {
      let client = createClient();
      let params = {
        localDir: tempManyFilesDir,
        deleteRemoved: true,
        s3Params: {
          Prefix: remoteManyFilesDir,
          Bucket: s3Bucket,
        },
      };
      let uploader = client.uploadDir(params);
      uploader.on('end', function() {
        // get a list of the remote files to ensure they all got created
        let params = {
          recursive: true,
          s3Params: {
            Bucket: s3Bucket,
            Prefix: remoteManyFilesDir,
          },
        };
        let client = createClient();
        let finder = client.listObjects(params);
        let found = false;
        finder.on('data', function(data) {
          strictEqual(data.Contents.length, 10);
          found = true;
        });
        finder.on('end', function() {
          strictEqual(found, true);
          done();
        });
      });
    });
  });

  it("multipart upload", function(done) {
    createBigFile(localFile, 16 * 1024 * 1024, function (err, _hexdigest) {
      if (err) return done(err);
      hexdigest = _hexdigest;
      let client = createClient();
      let params = {
        localFile: localFile,
        s3Params: {
          Key: remoteFile,
          Bucket: s3Bucket,
        },
      };
      let uploader = client.uploadFile(params);
      uploader.on('error', done);
      let progress = 0;
      let progressEventCount = 0;
      uploader.on('progress', function() {
        let amountDone = uploader.progressAmount;
        let amountTotal = uploader.progressTotal;
        let newProgress = amountDone / amountTotal;
        progressEventCount += 1;
        assert(newProgress >= progress, "old progress: " + progress + ", new progress: " + newProgress);
        progress = newProgress;
      });
      uploader.on('end', function(data) {
        strictEqual(progress, 1);
        assert(progressEventCount >= 2, "expected at least 2 progress events. got " + progressEventCount);
        ok(data, "expected data. got " + data);
        done();
      });
    });
  });

  it("download file with multipart etag", function(done) {
    doDownloadFileTest(done);
  });

  it("deletes a folder", function(done) {
    let client = createClient();
    let s3Params = {
      Prefix: remoteRoot,
      Bucket: s3Bucket,
    };
    let deleter = client.deleteDir(s3Params);
    deleter.on('end', function() {
      done();
    });
  });

  function doDownloadFileTest(done) {
    unlink(localFile, function(err) {
      if (err) return done(err);
      let client = createClient();
      let params = {
        localFile: localFile,
        s3Params: {
          Key: remoteFile,
          Bucket: s3Bucket,
        },
      };
      let downloader = client.downloadFile(params);
      downloader.on('error', done);
      let progress = 0;
      let progressEventCount = 0;
      let gotHttpHeaders = false;
      downloader.on('progress', function() {
        let amountDone = downloader.progressAmount;
        let amountTotal = downloader.progressTotal;
        let newProgress = amountDone / amountTotal;
        progressEventCount += 1;
        assert(newProgress >= progress, "old progress: " + progress + ", new progress: " + newProgress);
        progress = newProgress;
      });
      downloader.on('httpHeaders', function(statusCode, headers, resp) {
        let contentType = headers['content-type'];
        strictEqual(contentType, "image/png");
        gotHttpHeaders = true;
      });
      downloader.on('end', function() {
        strictEqual(progress, 1);
        assert(progressEventCount >= 3, "expected at least 3 progress events. got " + progressEventCount);
        let reader = createReadStream(localFile);
        let md5sum = createHash('md5');
        reader.on('data', function(data) {
          md5sum.update(data);
        });
        reader.on('end', function() {
          strictEqual(md5sum.digest('hex'), hexdigest);
          ok(gotHttpHeaders);
          unlink(localFile, done);
        });
      });
    });
  }
});

function assertFilesMd5(list, cb) {
  let pend = new Pend();
  list.forEach(function(o) {
    pend.go(function(cb) {
      let inStream = createReadStream(o.path);
      let hash = createHash('md5');
      inStream.pipe(hash);
      hash.on('data', function(digest) {
        let hexDigest = digest.toString('hex');
        strictEqual(hexDigest, o.md5, o.path + " md5 mismatch");
        cb();
      });
    });
  });
  pend.wait(cb);
}
