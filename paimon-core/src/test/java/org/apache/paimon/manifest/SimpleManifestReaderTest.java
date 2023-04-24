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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Partition;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RowDataPartitionComputer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** tests for {@link SimpleManifestReader}. */
public class SimpleManifestReaderTest {

    private final Object[][] partitionValues = {
        {"a", 1},
        {"B", 2},
        {"中国", 4},
        {"panda", 5}
    };

    private DataFileTestDataGenerator dgen = DataFileTestDataGenerator.builder().build();
    private final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());

    @TempDir java.nio.file.Path tempDir;

    private List<ManifestEntry> generateManifestEntriesWithAddOnly() {
        BinaryRow reuseRow = new BinaryRow(DEFAULT_PART_TYPE.getFieldCount());
        BinaryRowWriter reuseWriter = new BinaryRowWriter(reuseRow);
        List<ManifestEntry> entries = new ArrayList<>();
        for (Object[] td : partitionValues) {
            reuseWriter.reset();
            reuseWriter.writeRowKind(RowKind.INSERT);
            reuseWriter.writeString(0, BinaryString.fromString((String) td[0]));
            reuseWriter.writeInt(1, (int) td[1]);
            reuseWriter.complete();
            DataFileMeta meta = dgen.next().meta;
            entries.add(new ManifestEntry(FileKind.ADD, reuseRow.copy(), 0, 2, meta));
        }
        return entries;
    }

    private ManifestFile.Factory createManifestFileFactory(String pathStr) {
        Path path = new Path(pathStr);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString());
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        FileIO fileIO = FileIOFinder.find(path);
        return new ManifestFile.Factory(
                fileIO,
                new SchemaManager(fileIO, path),
                DEFAULT_PART_TYPE,
                avro,
                pathFactory,
                suggestedFileSize,
                null);
    }

    @Test
    public void testEntry() {
        List<ManifestEntry> entries = generateManifestEntriesWithAddOnly();
        ManifestFile.Factory factory = createManifestFileFactory(tempDir.toString());
        ManifestFile manifestFile = factory.create();
        List<ManifestFileMeta> actualMetas = manifestFile.write(entries);

        SimpleManifestReader simpleManifestReader = factory.createSimpleManifestReader();
        List<SimpleManifestEntry> simpleManifestEntryList =
                actualMetas.stream()
                        .flatMap(m -> simpleManifestReader.read(m.fileName()).stream())
                        .collect(Collectors.toList());

        AtomicInteger i = new AtomicInteger(0);

        entries.forEach(
                entry -> {
                    assertThat(entry.kind()).isEqualTo(simpleManifestEntryList.get(i.get()).kind());
                    assertThat(entry.partition())
                            .isEqualTo(simpleManifestEntryList.get(i.get()).partition());
                    assertThat(entry.bucket())
                            .isEqualTo(simpleManifestEntryList.get(i.get()).bucket());
                    assertThat(entry.file().fileName())
                            .isEqualTo(simpleManifestEntryList.get(i.getAndIncrement()).fileName());
                });
    }

    @Test
    public void testPartitions() {
        List<ManifestEntry> entries = generateManifestEntriesWithAddOnly();
        ManifestFile.Factory factory = createManifestFileFactory(tempDir.toString());
        ManifestFile manifestFile = factory.create();
        List<ManifestFileMeta> actualMetas = manifestFile.write(entries);

        SimpleManifestReader simpleManifestReader = factory.createSimpleManifestReader();
        Collection<SimpleManifestEntry> simpleManifestEntryList =
                actualMetas.stream()
                        .flatMap(m -> simpleManifestReader.read(m.fileName()).stream())
                        .collect(Collectors.toList());

        simpleManifestEntryList = AbstractManifestEntry.mergeEntries(simpleManifestEntryList);

        RowType rowType = simpleManifestReader.getPartitionType();

        RowDataPartitionComputer rowDataPartitionComputer =
                new RowDataPartitionComputer(
                        FileStorePathFactory.PARTITION_DEFAULT_NAME.defaultValue(),
                        rowType,
                        rowType.getFieldNames().toArray(new String[0]));

        List<Partition> partitions =
                simpleManifestEntryList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        SimpleManifestEntry::partition,
                                        LinkedHashMap::new,
                                        Collectors.reducing((a, b) -> b)))
                        .values()
                        .stream()
                        .map(Optional::get)
                        .map(
                                row ->
                                        Partition.of(
                                                rowDataPartitionComputer.generatePartValues(
                                                        row.partition())))
                        .collect(Collectors.toList());

        AtomicInteger i = new AtomicInteger(0);

        partitions.stream()
                .map(Partition::partitionSpec)
                .map(LinkedHashMap::values)
                .map(t -> t.toArray(new Object[0]))
                .map(t -> new Object[] {t[0], Integer.valueOf(t[1].toString())})
                .forEach(
                        v -> {
                            assertThat(v[0]).isEqualTo(partitionValues[i.get()][0]);
                            assertThat(v[1]).isEqualTo(partitionValues[i.getAndIncrement()][1]);
                        });
    }
}
