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

package org.apache.paimon.mergetree;

import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.data.BinaryRow.singleColumn;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LookupFile}. */
public class LookupFileTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testLocalFilePrefix() {
        RowType partType = RowType.of(DataTypes.STRING());
        assertThat(
                        localFilePrefix(
                                partType,
                                singleColumn("2024073105"),
                                10,
                                "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc"))
                .isEqualTo("2024073105-10-data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc");
        assertThat(
                        localFilePrefix(
                                RowType.of(),
                                EMPTY_ROW,
                                10,
                                "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc"))
                .isEqualTo("10-data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc");
        assertThat(
                        localFilePrefix(
                                partType,
                                singleColumn("2024073105-05-2123232313123123123213123"),
                                10,
                                "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc"))
                .isEqualTo(
                        "2024073105-05-212323-10-data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc");
    }

    @Test
    public void testCloseCleansLocalStateWhenReaderCloseFails() throws Exception {
        File localFile = tempDir.resolve("lookup-file").toFile();
        assertThat(localFile.createNewFile()).isTrue();
        AtomicBoolean callbackCalled = new AtomicBoolean(false);
        LookupFile lookupFile =
                new LookupFile(
                        localFile,
                        1,
                        0L,
                        "v1",
                        new LookupStoreReader() {
                            @Override
                            public byte[] lookup(byte[] key) {
                                return null;
                            }

                            @Override
                            public void close() throws IOException {
                                throw new IOException("reader close failed");
                            }
                        },
                        () -> callbackCalled.set(true));

        assertThatThrownBy(() -> lookupFile.close(RemovalCause.SIZE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("reader close failed");
        assertThat(callbackCalled).isTrue();
        assertThat(localFile).doesNotExist();
    }
}
