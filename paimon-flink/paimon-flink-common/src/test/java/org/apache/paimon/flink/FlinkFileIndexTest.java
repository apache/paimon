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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** ITCase for FileIndex. */
public class FlinkFileIndexTest extends CatalogITCaseBase {

    @Test
    public void testDynamicBloomFileIndex() throws Catalog.TableNotExistException, IOException {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '-1',"
                        + " 'file-index.dynamic-bloom-filter.columns'='k,v'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '100', 15, '20221208'), (1, '100', 16, '20221208'), (1, '100', 15, '20221209')");

        FileStoreTable table = paimonTable("T");
        List<ManifestEntry> list = table.store().newScan().plan().files();
        for (ManifestEntry entry : list) {
            List<String> extraFiles =
                    entry.file().extraFiles().stream()
                            .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());

            Assertions.assertThat(extraFiles.size()).isEqualTo(1);

            String file = extraFiles.get(0);

            Path indexFilePath =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toAlignedPath(file, entry.file());
            try (FileIndexFormat.Reader reader =
                    FileIndexFormat.createReader(
                            table.fileIO().newInputStream(indexFilePath), table.rowType())) {
                Set<FileIndexReader> readerSetK = reader.readColumnIndex("k");
                Assertions.assertThat(readerSetK.size()).isEqualTo(1);

                Predicate predicateK = new PredicateBuilder(table.rowType()).equal(0, 1);
                for (FileIndexReader fileIndexReader : readerSetK) {
                    Assertions.assertThat(predicateK.visit(fileIndexReader).remain()).isTrue();
                }

                predicateK = new PredicateBuilder(table.rowType()).equal(0, 4);
                for (FileIndexReader fileIndexReader : readerSetK) {
                    Assertions.assertThat(predicateK.visit(fileIndexReader).remain()).isFalse();
                }

                Set<FileIndexReader> readerSetV = reader.readColumnIndex("v");
                Assertions.assertThat(readerSetV.size()).isEqualTo(1);

                Predicate predicateV =
                        new PredicateBuilder(table.rowType())
                                .equal(1, BinaryString.fromString("100"));
                for (FileIndexReader fileIndexReader : readerSetV) {
                    Assertions.assertThat(predicateV.visit(fileIndexReader).remain()).isTrue();
                }

                predicateV =
                        new PredicateBuilder(table.rowType())
                                .equal(1, BinaryString.fromString("101"));
                for (FileIndexReader fileIndexReader : readerSetV) {
                    Assertions.assertThat(predicateV.visit(fileIndexReader).remain()).isFalse();
                }
            }
        }
    }
}
