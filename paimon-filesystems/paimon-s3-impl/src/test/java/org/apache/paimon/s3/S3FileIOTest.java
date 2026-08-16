/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.s3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOBehaviorTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behavior tests for {@link S3FileIO}, backed by a MinIO container. Exercises the file system
 * contract with credentials and, separately, credential-less (anonymous) access.
 */
class S3FileIOTest extends FileIOBehaviorTestBase {

    private static final String TEMPORARY_PROVIDER =
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

    private static final String DEFAULT_PROVIDER =
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider";

    @RegisterExtension
    private static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    @Override
    protected FileIO getFileSystem() {
        return createFileIO(MINIO_CONTAINER.getS3ConfigOptions());
    }

    @Override
    protected Path getBasePath() {
        return new Path(MINIO_CONTAINER.getS3UriForDefaultBucket() + "/test");
    }

    private static S3FileIO createFileIO(Map<String, String> options) {
        S3FileIO fileIO = new S3FileIO();
        fileIO.configure(CatalogContext.create(Options.fromMap(options)));
        return fileIO;
    }

    /** Basic round-trip using long-lived access key / secret key credentials. */
    @Test
    void testAccessKeyAndSecretKey() throws Exception {
        FileIO fileIO = createFileIO(MINIO_CONTAINER.getS3ConfigOptions());
        assertThat(fileIO.isObjectStore()).isTrue();

        Path file = new Path(getBasePath(), "keyed-" + randomName());
        fileIO.writeFile(file, "keyed-payload", true);

        assertThat(fileIO.exists(file)).isTrue();
        assertThat(fileIO.readFileUtf8(file)).isEqualTo("keyed-payload");

        fileIO.delete(file, false);
        assertThat(fileIO.exists(file)).isFalse();
    }

    /**
     * Round-trip using temporary credentials (access key, secret key and session token) issued by
     * MinIO's STS endpoint, resolved through the S3A {@code TemporaryAWSCredentialsProvider}.
     */
    @Test
    void testSessionTokenAuth() throws Exception {
        Map<String, String> options =
                new HashMap<>(MINIO_CONTAINER.getS3ConfigOptionsWithSessionToken());
        options.put("s3.aws.credentials.provider", TEMPORARY_PROVIDER);

        FileIO fileIO = createFileIO(options);
        assertThat(fileIO.isObjectStore()).isTrue();

        Path file = new Path(getBasePath(), "sts-" + randomName());
        fileIO.writeFile(file, "sts-payload", true);

        assertThat(fileIO.exists(file)).isTrue();
        assertThat(fileIO.readFileUtf8(file)).isEqualTo("sts-payload");

        fileIO.delete(file, false);
        assertThat(fileIO.exists(file)).isFalse();
    }

    /**
     * S3 must work without any credentials in the Paimon options, discovering them through the AWS
     * default credential provider chain. Production supplies them via environment variables; a unit
     * test cannot mutate the JVM environment, so the equivalent JVM system properties are used
     * instead — the default chain reads both.
     */
    @Test
    void testDefaultCredentialsProviderChain() throws Exception {
        Map<String, String> base = MINIO_CONTAINER.getS3ConfigOptions();

        System.setProperty("aws.accessKeyId", base.get("s3.access.key"));
        System.setProperty("aws.secretAccessKey", base.get("s3.secret.key"));
        try {
            Map<String, String> options = new HashMap<>();
            options.put("s3.endpoint", base.get("s3.endpoint"));
            options.put("s3.path.style.access", "true");
            // No access/secret key here: let the AWS default chain discover the credentials.
            options.put("s3.aws.credentials.provider", DEFAULT_PROVIDER);

            FileIO fileIO = createFileIO(options);
            assertThat(fileIO.isObjectStore()).isTrue();

            Path file = new Path(getBasePath(), "default-chain-" + randomName());
            fileIO.writeFile(file, "default-chain-payload", true);

            assertThat(fileIO.exists(file)).isTrue();
            assertThat(fileIO.readFileUtf8(file)).isEqualTo("default-chain-payload");

            fileIO.delete(file, false);
            assertThat(fileIO.exists(file)).isFalse();
        } finally {
            System.clearProperty("aws.accessKeyId");
            System.clearProperty("aws.secretAccessKey");
        }
    }
}
