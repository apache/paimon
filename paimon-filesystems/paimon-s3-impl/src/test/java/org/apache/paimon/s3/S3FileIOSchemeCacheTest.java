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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that one {@link S3FileIO} instance serves both {@code s3://} and {@code s3a://} paths of
 * the same bucket. No S3 backend is contacted: the bucket probe is disabled and the endpoint is
 * unroutable, and only {@link FileSystem#makeQualified} is called on the returned file systems.
 */
class S3FileIOSchemeCacheTest {

    private static final String BUCKET = "paimon-scheme-cache-bucket";

    @Test
    void testS3ThenS3aOnSameBucket() throws Exception {
        assertSchemesAreServedSeparately("s3", "s3a");
    }

    @Test
    void testS3aThenS3OnSameBucket() throws Exception {
        assertSchemesAreServedSeparately("s3a", "s3");
    }

    @Test
    void testSameSchemeAndBucketReusesFileSystem() throws Exception {
        S3FileIO fileIO = newFileIO();
        for (String scheme : new String[] {"s3", "s3a"}) {
            FileSystem first = fileIO.getFileSystem(path(scheme, "warehouse/a"));
            FileSystem second = fileIO.getFileSystem(path(scheme, "warehouse/b"));
            assertThat(second).isSameAs(first);
        }
    }

    private static void assertSchemesAreServedSeparately(String firstScheme, String secondScheme)
            throws Exception {
        S3FileIO fileIO = newFileIO();
        Path firstPath = path(firstScheme, "warehouse/db/t");
        Path secondPath = path(secondScheme, "hive/t/part-0.parquet");

        FileSystem firstFs = fileIO.getFileSystem(firstPath);
        FileSystem secondFs = fileIO.getFileSystem(secondPath);

        // makeQualified runs FileSystem#checkPath, which rejects a path of another scheme.
        assertThat(firstFs.makeQualified(firstPath)).isEqualTo(firstPath);
        assertThat(secondFs.makeQualified(secondPath)).isEqualTo(secondPath);
        assertThat(firstFs.getUri().getScheme()).isEqualTo(firstScheme);
        assertThat(secondFs.getUri().getScheme()).isEqualTo(secondScheme);
        assertThat(secondFs).isNotSameAs(firstFs);
    }

    private static S3FileIO newFileIO() {
        Options options = new Options();
        // Never reach the network: no bucket probe, dummy credentials and an unroutable endpoint.
        options.set("s3.bucket.probe", "0");
        options.set("s3.endpoint", "http://localhost:1");
        options.set("s3.endpoint.region", "us-east-1");
        options.set("s3.access-key", "dummy-access-key");
        options.set("s3.secret-key", "dummy-secret-key");
        S3FileIO fileIO = new S3FileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }

    private static Path path(String scheme, String key) {
        return new Path(scheme + "://" + BUCKET + "/" + key);
    }
}
