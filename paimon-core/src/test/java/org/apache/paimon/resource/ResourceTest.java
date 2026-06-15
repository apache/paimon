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

package org.apache.paimon.resource;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.UriReaderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Resource}. */
public class ResourceTest {

    @TempDir Path tempDir;

    @Test
    public void testReadLocalResource() throws Exception {
        byte[] data = new byte[] {1, 2, 3, 4};
        Path file = tempDir.resolve("resource.bin");
        Files.write(file, data);

        Resource resource =
                Resource.toResource(
                        ResourceType.FILE,
                        Identifier.create("default", "resource"),
                        null,
                        file.toString(),
                        data.length,
                        Files.getLastModifiedTime(file).toMillis(),
                        new UriReaderFactory(CatalogContext.create(new Options())));

        assertThat(resource.toBytes()).isEqualTo(data);
        try (SeekableInputStream in = resource.newInputStream()) {
            assertThat(in.read()).isEqualTo(1);
        }
    }
}
