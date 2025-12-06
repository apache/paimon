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

package org.apache.paimon.format.lance;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PluginFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanceUtilsTest {

    private static class TestFileIO extends PluginFileIO {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            throw new UnsupportedOperationException("Not used in tests");
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        // Set options directly to avoid Hadoop dependency
        void setOptions(Options opts) {
            this.options = opts;
        }
    }

    @Test
    void testOssUrlConversion() {
        Path path = new Path("oss://test-bucket/db-name.db/table-name/bucket-0/data.lance");
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-example-region.example.com");
        options.set("fs.oss.accessKeyId", "test-key");
        options.set("fs.oss.accessKeySecret", "test-secret");

        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        Pair<Path, Map<String, String>> result = LanceUtils.toLanceSpecified(fileIO, path);

        // Keep oss:// scheme (same as Python implementation)
        assertTrue(result.getKey().toString().startsWith("oss://test-bucket/"));

        Map<String, String> storageOptions = result.getValue();
        assertEquals(
                "https://test-bucket.oss-example-region.example.com",
                storageOptions.get("endpoint"));
        assertEquals("test-key", storageOptions.get("access_key_id"));
        assertEquals("test-secret", storageOptions.get("secret_access_key"));
        assertEquals("true", storageOptions.get("virtual_hosted_style_request"));

        assertTrue(storageOptions.containsKey("fs.oss.endpoint"));
        assertTrue(storageOptions.containsKey("fs.oss.accessKeyId"));
        assertTrue(storageOptions.containsKey("fs.oss.accessKeySecret"));
    }

    @Test
    void testOssUrlWithSecurityToken() {
        Path path = new Path("oss://my-bucket/path/to/file.lance");
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-example-region.example.com");
        options.set("fs.oss.accessKeyId", "test-access-key");
        options.set("fs.oss.accessKeySecret", "test-secret-key");
        options.set("fs.oss.securityToken", "test-token");

        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        Pair<Path, Map<String, String>> result = LanceUtils.toLanceSpecified(fileIO, path);

        Map<String, String> storageOptions = result.getValue();
        assertEquals("test-token", storageOptions.get("session_token"));
        assertEquals("test-token", storageOptions.get("oss_session_token"));
        assertTrue(storageOptions.containsKey("fs.oss.securityToken"));
    }
}
