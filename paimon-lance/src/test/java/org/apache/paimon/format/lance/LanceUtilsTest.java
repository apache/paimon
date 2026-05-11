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

        void setOptions(Options opts) {
            this.options = opts;
        }
    }

    @Test
    void testOssUrlConversion() {
        Path path = new Path("oss://test-bucket/db-name.db/table-name/bucket-0/data.lance");
        Options options = new Options();
        options.set(LanceUtils.FS_OSS_ENDPOINT, "oss-example-region.example.com");
        options.set(LanceUtils.FS_OSS_ACCESS_KEY_ID, "test-key");
        options.set(LanceUtils.FS_OSS_ACCESS_KEY_SECRET, "test-secret");

        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        Pair<Path, Map<String, String>> result = LanceUtils.toLanceSpecifiedForReader(fileIO, path);

        assertTrue(result.getKey().toString().startsWith("oss://test-bucket/"));

        Map<String, String> storageOptions = result.getValue();
        assertEquals(
                "https://test-bucket.oss-example-region.example.com",
                storageOptions.get(LanceUtils.STORAGE_OPTION_ENDPOINT));
        assertEquals("test-key", storageOptions.get(LanceUtils.STORAGE_OPTION_ACCESS_KEY_ID));
        assertEquals(
                "test-secret", storageOptions.get(LanceUtils.STORAGE_OPTION_SECRET_ACCESS_KEY));
        assertEquals("true", storageOptions.get(LanceUtils.STORAGE_OPTION_VIRTUAL_HOSTED_STYLE));

        assertTrue(storageOptions.containsKey(LanceUtils.FS_OSS_ENDPOINT));
        assertTrue(storageOptions.containsKey(LanceUtils.FS_OSS_ACCESS_KEY_ID));
        assertTrue(storageOptions.containsKey(LanceUtils.FS_OSS_ACCESS_KEY_SECRET));
    }

    @Test
    void testOssUrlWithSecurityToken() {
        Path path = new Path("oss://my-bucket/path/to/file.lance");
        Options options = new Options();
        options.set(LanceUtils.FS_OSS_ENDPOINT, "oss-example-region.example.com");
        options.set(LanceUtils.FS_OSS_ACCESS_KEY_ID, "test-access-key");
        options.set(LanceUtils.FS_OSS_ACCESS_KEY_SECRET, "test-secret-key");
        options.set(LanceUtils.FS_OSS_SECURITY_TOKEN, "test-token");

        TestFileIO fileIO = new TestFileIO();
        fileIO.setOptions(options);

        Pair<Path, Map<String, String>> result = LanceUtils.toLanceSpecifiedForReader(fileIO, path);

        Map<String, String> storageOptions = result.getValue();
        assertEquals("test-token", storageOptions.get(LanceUtils.STORAGE_OPTION_SESSION_TOKEN));
        assertEquals("test-token", storageOptions.get(LanceUtils.STORAGE_OPTION_OSS_SESSION_TOKEN));
        assertTrue(storageOptions.containsKey(LanceUtils.FS_OSS_SECURITY_TOKEN));
    }
}
