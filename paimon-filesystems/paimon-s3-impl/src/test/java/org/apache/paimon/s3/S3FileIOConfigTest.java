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
import org.apache.paimon.options.Options;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Hadoop configuration that {@link S3FileIO} derives from catalog options. No S3
 * backend is contacted: {@code fs.s3a.bucket.probe=0} skips the bucket existence check and the
 * endpoint is unroutable.
 */
class S3FileIOConfigTest {

    @Test
    void testHyphenSessionTokenIsMirrored() throws Exception {
        Options options = baseOptions();
        options.set("s3.session-token", "hyphen-token");

        S3AFileSystem fs = fileSystem(options, "paimon-hyphen-token-bucket");

        assertThat(fs.getConf().get("fs.s3a.session.token")).isEqualTo("hyphen-token");
        try (AWSCredentialProviderList providers = fs.shareCredentials("test")) {
            AwsCredentials credentials = providers.resolveCredentials();
            assertThat(credentials).isInstanceOf(AwsSessionCredentials.class);
            assertThat(((AwsSessionCredentials) credentials).sessionToken())
                    .isEqualTo("hyphen-token");
        }
    }

    @Test
    void testDotSessionTokenIsKept() throws Exception {
        Options options = baseOptions();
        options.set("s3.session.token", "dot-token");

        S3AFileSystem fs = fileSystem(options, "paimon-dot-token-bucket");

        assertThat(fs.getConf().get("fs.s3a.session.token")).isEqualTo("dot-token");
    }

    private static S3AFileSystem fileSystem(Options options, String bucket) throws Exception {
        S3FileIO fileIO = new S3FileIO();
        fileIO.configure(CatalogContext.create(options));
        return (S3AFileSystem) fileIO.getFileSystem(new Path("s3://" + bucket + "/"));
    }

    private static Options baseOptions() {
        Options options = new Options();
        options.set("s3.bucket.probe", "0");
        options.set("s3.endpoint", "http://localhost:1");
        options.set("s3.endpoint.region", "us-east-1");
        options.set("s3.access-key", "dummy-access-key");
        options.set("s3.secret-key", "dummy-secret-key");
        return options;
    }
}
