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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link FileIO}. */
public class FileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRequireOptions() throws IOException {
        Options options = new Options();

        try {
            FileIO.get(
                    new Path("require-options://" + tempDir.toString()),
                    CatalogContext.create(options));
            Assertions.fail();
        } catch (UnsupportedSchemeException e) {
            assertThat(e.getSuppressed()[0])
                    .hasMessageContaining("Missing required options are:\n\nRequire1\nreQuire2");
        }

        options.set("Require1", "dummy");
        options.set("reQuire2", "dummy");
        FileIO fileIO =
                FileIO.get(
                        new Path("require-options://" + tempDir.toString()),
                        CatalogContext.create(options));
        assertThat(fileIO).isInstanceOf(RequireOptionsFileIOLoader.MyFileIO.class);
    }
}
