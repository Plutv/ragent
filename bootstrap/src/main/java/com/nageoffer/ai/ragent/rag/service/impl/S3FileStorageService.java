/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.rag.service.impl;

import cn.hutool.core.lang.Assert;
import com.nageoffer.ai.ragent.framework.errorcode.BaseErrorCode;
import com.nageoffer.ai.ragent.framework.exception.ServiceException;
import com.nageoffer.ai.ragent.rag.dto.StoredFileDTO;
import com.nageoffer.ai.ragent.rag.service.FileStorageService;
import com.nageoffer.ai.ragent.rag.util.FileTypeDetector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3FileStorageService implements FileStorageService {

    private static final Tika TIKA = new Tika();
    private static final Duration PRESIGN_DURATION = Duration.ofMinutes(10);
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 60_000;

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;

    @Override
    public StoredFileDTO upload(String bucketName, MultipartFile file) {
        validateBucketName(bucketName);
        Assert.isFalse(file == null || file.isEmpty(), "上传文件不能为空");
        ensureBucketExists(bucketName);

        String originalFilename = file.getOriginalFilename();
        long size = file.getSize();

        String detectedContentType;
        try (InputStream is = file.getInputStream()) {
            detectedContentType = TIKA.detect(is, originalFilename);
        } catch (IOException e) {
            throw storageException("读取上传文件失败", e);
        }

        try (InputStream is = file.getInputStream()) {
            return streamUploadToS3(bucketName, is, size, originalFilename, detectedContentType);
        } catch (IOException e) {
            throw storageException("上传文件到对象存储失败", e);
        }
    }

    @Override
    public StoredFileDTO upload(String bucketName, InputStream content, long size, String originalFilename, String contentType) {
        validateBucketName(bucketName);
        Assert.notNull(content, "上传内容不能为空");
        Assert.isTrue(size >= 0, "上传内容大小不能小于 0");
        ensureBucketExists(bucketName);

        String detected = resolveContentType(originalFilename, contentType);
        try {
            return streamUploadToS3(bucketName, content, size, originalFilename, detected);
        } catch (IOException e) {
            throw storageException("上传文件流到对象存储失败", e);
        }
    }

    @Override
    public StoredFileDTO upload(String bucketName, byte[] content, String originalFilename, String contentType) {
        validateBucketName(bucketName);
        Assert.notNull(content, "上传内容不能为空");
        ensureBucketExists(bucketName);

        String detected = resolveContentType(originalFilename, contentType);
        try {
            return streamUploadToS3(bucketName, new ByteArrayInputStream(content), content.length, originalFilename, detected);
        } catch (IOException e) {
            throw storageException("上传字节内容到对象存储失败", e);
        }
    }

    @Override
    public InputStream openStream(String url) {
        S3Location loc = parseS3Url(url);
        return s3Client.getObject(b -> b.bucket(loc.bucket()).key(loc.key()));
    }

    @Override
    public void deleteByUrl(String url) {
        S3Location loc = parseS3Url(url);
        s3Client.deleteObject(b -> b.bucket(loc.bucket()).key(loc.key()));
    }

    private StoredFileDTO streamUploadToS3(String bucketName, InputStream inputStream,
                                           long size, String originalFilename,
                                           String detectedContentType) throws IOException {
        String s3Key = generateS3Key(originalFilename);

        PresignedPutObjectRequest presignedReq = s3Presigner.presignPutObject(p -> p
                .signatureDuration(PRESIGN_DURATION)
                .putObjectRequest(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .contentType(detectedContentType)
                        .build()));

        streamPutViaPresignedUrl(presignedReq, inputStream, size, detectedContentType);

        String url = toS3Url(bucketName, s3Key);
        return buildStoredFileDTO(url, originalFilename, detectedContentType, size);
    }

    @Override
    public StoredFileDTO reliableUpload(String bucketName, InputStream content, long size,
                                        String originalFilename, String contentType) {
        validateBucketName(bucketName);
        Assert.notNull(content, "上传内容不能为空");
        Assert.isTrue(size >= 0, "上传内容大小不能小于 0");
        ensureBucketExists(bucketName);

        String detected = resolveContentType(originalFilename, contentType);
        String s3Key = generateS3Key(originalFilename);

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .contentType(detected)
                        .build(),
                RequestBody.fromInputStream(content, size));

        String url = toS3Url(bucketName, s3Key);
        return buildStoredFileDTO(url, originalFilename, detected, size);
    }

    private void ensureBucketExists(String bucketName) {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucketName));
            return;
        } catch (NoSuchBucketException e) {
            log.warn("对象存储桶不存在，准备自动创建，bucketName={}", bucketName);
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw storageException("检查对象存储桶失败: " + bucketName, e);
            }
            log.warn("对象存储桶返回 404，准备自动创建，bucketName={}", bucketName);
        }

        try {
            s3Client.createBucket(builder -> builder.bucket(bucketName));
            log.info("自动创建对象存储桶成功，bucketName={}", bucketName);
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            log.info("对象存储桶已存在，跳过创建，bucketName={}", bucketName);
        } catch (S3Exception e) {
            throw storageException("创建对象存储桶失败: " + bucketName, e);
        }
    }

    private void streamPutViaPresignedUrl(PresignedPutObjectRequest presignedReq,
                                          InputStream inputStream,
                                          long size,
                                          String contentType) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) presignedReq.url().openConnection();
        try {
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setFixedLengthStreamingMode(size);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);

            presignedReq.signedHeaders()
                    .forEach((k, vs) -> vs.forEach(v -> conn.addRequestProperty(k, v)));

            if (contentType != null && !contentType.isBlank()) {
                conn.setRequestProperty("Content-Type", contentType);
            }

            try (OutputStream out = conn.getOutputStream()) {
                inputStream.transferTo(out);
            }

            int code = conn.getResponseCode();
            if (code < 200 || code >= 300) {
                String errorBody = readErrorStream(conn);
                throw new IOException(String.format(
                        "S3 流式上传失败: HTTP %d, url=%s, body=%s",
                        code, presignedReq.url(), errorBody));
            }
        } finally {
            conn.disconnect();
        }
    }

    private String readErrorStream(HttpURLConnection conn) {
        try (InputStream err = conn.getErrorStream()) {
            return err != null ? new String(err.readAllBytes(), StandardCharsets.UTF_8) : "(empty)";
        } catch (IOException e) {
            return "(read error: " + e.getMessage() + ")";
        }
    }

    private String toS3Url(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

    private S3Location parseS3Url(String url) {
        URI uri = URI.create(url);
        if (!"s3".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Unsupported url scheme: " + url);
        }
        String bucket = uri.getHost();
        String path = uri.getPath();
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("Invalid s3 url (bucket missing): " + url);
        }
        String key = (path != null && path.startsWith("/")) ? path.substring(1) : path;
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Invalid s3 url (key missing): " + url);
        }
        return new S3Location(bucket, key);
    }

    private String extractSuffix(String filename) {
        if (filename == null) {
            return "";
        }
        int idx = filename.lastIndexOf('.');
        return (idx < 0 || idx == filename.length() - 1) ? "" : filename.substring(idx + 1).trim();
    }

    private String generateS3Key(String originalFilename) {
        String suffix = extractSuffix(originalFilename);
        UUID uuid = UUID.randomUUID();
        String key = String.format("%016x%016x", uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        return suffix.isBlank() ? key : key + "." + suffix;
    }

    private void validateBucketName(String bucketName) {
        Assert.notBlank(bucketName, "bucketName 不能为空");
    }

    private StoredFileDTO buildStoredFileDTO(String url, String originalFilename,
                                             String contentType, long size) {
        String detectedType = FileTypeDetector.detectType(originalFilename, contentType);
        return StoredFileDTO.builder()
                .url(url)
                .detectedType(detectedType)
                .size(size)
                .originalFilename(originalFilename)
                .build();
    }

    private String resolveContentType(String originalFilename, String contentType) {
        if (contentType != null && !contentType.isBlank()) {
            return contentType;
        }
        if (originalFilename != null && !originalFilename.isBlank()) {
            return TIKA.detect(originalFilename);
        }
        return null;
    }

    private ServiceException storageException(String message, Throwable cause) {
        return new ServiceException(message, cause, BaseErrorCode.SERVICE_ERROR);
    }

    private record S3Location(String bucket, String key) {
    }
}
