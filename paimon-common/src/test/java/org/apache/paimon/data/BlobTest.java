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

package org.apache.paimon.data;

import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Blob}. */
public class BlobTest {

    @TempDir java.nio.file.Path tempPath;

    private String file;

    @BeforeEach
    void beforeEach() throws IOException {
        file = new File(tempPath.toString(), "test.txt").getAbsolutePath();
        Files.write(Paths.get(file), "test data".getBytes());
    }

    @Test
    public void testFromData() {
        byte[] testData = "test data".getBytes();
        Blob blob = Blob.fromData(testData);
        assertThat(blob).isInstanceOf(BlobData.class);
        assertThat(blob.toData()).isEqualTo(testData);
    }

    @Test
    public void testFromLocal() {
        Blob blob = Blob.fromLocal(file);
        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toData()).isEqualTo("test data".getBytes());
    }

    @Test
    public void testFromFile() {
        Blob blob = Blob.fromFile(LocalFileIO.create(), file, 0, 4);
        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toData()).isEqualTo("test".getBytes());
    }

    @Test
    public void testFromPath() {
        Blob blob = Blob.fromFile(LocalFileIO.create(), file);
        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toData()).isEqualTo("test data".getBytes());
    }

    @Test
    public void testFromHttp() {
        String uri = "http://example.com/file.txt";
        Blob blob = Blob.fromHttp(uri);
        assertThat(blob).isInstanceOf(BlobRef.class);
    }
}
