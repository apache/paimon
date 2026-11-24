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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TemplateEntropyInjectExternalPathProvider}. */
public class TemplateEntropyInjectExternalPathProviderTest {

    private static final Path RELATIVE_BUCKET_PATH = new Path("dt=2026-11-02/bucket-0");

    @Test
    public void testHashIsInjectedBeforeTableContext() {
        TemplateEntropyInjectExternalPathProvider provider =
                new TemplateEntropyInjectExternalPathProvider(
                        Collections.singletonList(
                                new Path("s3://bucket-a/{hash}/my_db.db/my_table")),
                        RELATIVE_BUCKET_PATH);

        for (int i = 0; i < 20; i++) {
            String fileName = "data-" + i + ".parquet";
            String hashDirs = EntropyInjectExternalPathProvider.computeHash(fileName);
            assertThat(provider.getNextExternalDataPath(fileName))
                    .isEqualTo(
                            new Path(
                                    "s3://bucket-a/"
                                            + hashDirs
                                            + "/my_db.db/my_table/dt=2026-11-02/bucket-0/"
                                            + fileName));
        }
    }

    @Test
    public void testTablesShareTheSamePrefix() {
        String fileName = "data-0.parquet";
        String hashDirs = EntropyInjectExternalPathProvider.computeHash(fileName);

        Path first =
                new TemplateEntropyInjectExternalPathProvider(
                                Collections.singletonList(
                                        new Path("s3://bucket-a/{hash}/db1.db/table1")),
                                new Path("bucket-0"))
                        .getNextExternalDataPath(fileName);
        Path second =
                new TemplateEntropyInjectExternalPathProvider(
                                Collections.singletonList(
                                        new Path("s3://bucket-a/{hash}/db2.db/table2")),
                                new Path("bucket-0"))
                        .getNextExternalDataPath(fileName);

        String sharedPrefix = "s3://bucket-a/" + hashDirs;
        assertThat(first.toString()).startsWith(sharedPrefix + "/db1.db/table1/");
        assertThat(second.toString()).startsWith(sharedPrefix + "/db2.db/table2/");
    }

    @Test
    public void testMultipleTemplatesRejected() {
        List<Path> templates =
                Arrays.asList(
                        new Path("s3://bucket-a/{hash}/my_db.db/my_table"),
                        new Path("s3://bucket-b/{hash}/my_db.db/my_table"));

        assertThatThrownBy(
                        () ->
                                new TemplateEntropyInjectExternalPathProvider(
                                        templates, new Path("bucket-0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a single external path template");
    }

    @Test
    public void testTemplateWithoutPlaceholder() {
        assertThatThrownBy(
                        () ->
                                new TemplateEntropyInjectExternalPathProvider(
                                        Collections.singletonList(
                                                new Path("s3://bucket-a/my_db.db/my_table")),
                                        RELATIVE_BUCKET_PATH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("{hash}");
    }
}
