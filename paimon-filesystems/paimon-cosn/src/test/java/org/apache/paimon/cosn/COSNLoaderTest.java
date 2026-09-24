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

package org.apache.paimon.cosn;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link COSNLoader}. */
public class COSNLoaderTest {

    private static final Path COSN_PATH = new Path("cosn://bucket/path");

    @Test
    public void testEndpointSuffixSelectsPluginWithDefaultCredentialChain() throws IOException {
        Options options = new Options();
        options.set("fs.cosn.bucket.endpoint_suffix", "cos.example.com");

        assertCosnPluginSelected(options);
    }

    @Test
    public void testEndpointSuffixSelectsPluginWithCustomCredentialProvider() throws IOException {
        Options options = new Options();
        options.set("fs.cosn.bucket.endpoint_suffix", "cos.example.com");
        options.set("fs.cosn.credentials.provider", "example.CustomCredentialsProvider");

        assertCosnPluginSelected(options);
    }

    @Test
    public void testRegionSelectsPluginWithoutStaticCredentials() throws IOException {
        Options options = new Options();
        options.set("fs.cosn.bucket.region", "ap-test");

        assertCosnPluginSelected(options);
    }

    private static void assertCosnPluginSelected(Options options) throws IOException {
        FileIO fileIO = FileIO.get(COSN_PATH, CatalogContext.create(options));

        assertThat(fileIO).isInstanceOf(PluginFileIO.class);
        assertThat(fileIO.getClass().getEnclosingClass()).isEqualTo(COSNLoader.class);
    }
}
