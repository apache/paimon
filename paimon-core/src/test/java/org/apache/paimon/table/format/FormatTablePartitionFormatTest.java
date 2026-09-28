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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests partition format inheritance, file planning and reads through the shared core path. */
class FormatTablePartitionFormatTest {

    private static final RowType DATA_TYPE =
            RowType.builder().field("value", DataTypes.STRING()).build();
    private static final RowType TABLE_TYPE =
            RowType.builder()
                    .field("value", DataTypes.STRING())
                    .field("pt", DataTypes.STRING())
                    .build();

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testDefaultsAndExplicitInvalidValues() {
        assertThat(FormatTablePartitionOptions.fileFormat(Collections.emptyMap(), null))
                .isEqualTo("parquet");
        assertThat(FormatTablePartitionOptions.fileFormat(options("orc"), Collections.emptyMap()))
                .isEqualTo("orc");
        assertThat(FormatTablePartitionOptions.fileFormat(options("orc"), options("parquet")))
                .isEqualTo("parquet");
        assertThat(FormatTablePartitionOptions.fileFormat(Collections.emptyMap(), options("ORC")))
                .isEqualTo("orc");
        for (String invalid : Arrays.asList(null, "", " ", "avro", "unknown")) {
            assertThatThrownBy(
                            () ->
                                    FormatTablePartitionOptions.fileFormat(
                                            options("orc"), options(invalid)))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(FormatTable.Format.class)
    void testEveryTableFormatCanBeInheritedOrOverridden(FormatTable.Format format) {
        String expected = format.name().toLowerCase(java.util.Locale.ROOT);
        assertThat(FormatTablePartitionOptions.fileFormat(options(format.name()), null))
                .isEqualTo(expected);
        assertThat(
                        FormatTablePartitionOptions.fileFormat(
                                options("parquet"), options(format.name())))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "csv", "json", "text"})
    void testMixedPartitionReadsAndStatistics(String override) throws Exception {
        String inherited = override.equals("orc") ? "parquet" : "orc";
        Path tablePath = new Path(tempDir.toUri());
        writeFile(tablePath, "inherited", inherited, "base");
        writeFile(tablePath, "overridden", override, "first", "second");
        List<Partition> partitions =
                Arrays.asList(
                        partition("inherited", null), partition("overridden", options(override)));
        FormatTable table = table(tablePath, inherited, partitions);

        ReadBuilder read = table.newReadBuilder();
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).hasSize(2);
        for (Split split : splits) {
            FormatDataSplit copy = InstantiationUtil.clone((FormatDataSplit) split);
            assertThat(copy).isEqualTo(split);
            assertThat(copy.fileFormat()).isIn(inherited, override);
        }
        assertThat(readRows(read, splits)).containsExactlyInAnyOrder("base", "first", "second");

        ReadBuilder selected =
                table.newReadBuilder()
                        .withFilter(
                                new PredicateBuilder(TABLE_TYPE)
                                        .equal(1, BinaryString.fromString("overridden")))
                        .withReadType(DATA_TYPE);
        assertThat(readRows(selected, selected.newScan().plan().splits()))
                .containsExactly("first", "second");

        List<PartitionStatistics> statistics =
                new FormatTablePartitionStatsCollector(table, true, 2)
                        .collectPartitions(partitions);
        assertThat(statistics.get(0).recordCount()).isEqualTo(1);
        assertThat(statistics.get(1).recordCount())
                .isEqualTo(
                        override.equals("orc") || override.equals("parquet")
                                ? 2
                                : PartitionStatistics.UNKNOWN);
        assertThat(statistics).allSatisfy(stat -> assertThat(stat.fileCount()).isEqualTo(1));

        // A legacy split must inherit ORC, rather than accidentally taking the global Parquet
        // default.
        if (inherited.equals("orc")) {
            FormatDataSplit base =
                    (FormatDataSplit)
                            splits.stream()
                                    .filter(
                                            split ->
                                                    ((FormatDataSplit) split)
                                                            .fileFormat()
                                                            .equals("orc"))
                                    .findFirst()
                                    .get();
            FormatDataSplit legacy = new FormatDataSplit(base.files(), base.partition());
            assertThat(readRows(read, Collections.singletonList(legacy))).containsExactly("base");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"csv", "json"})
    void testSplittingUsesPartitionFormat(String format) throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        writeFile(tablePath, "override", format, "first", "second", "third", "fourth");
        FormatTable table =
                table(
                                tablePath,
                                "parquet",
                                Collections.singletonList(partition("override", options(format))))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "8 b"));
        ReadBuilder read = table.newReadBuilder();
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits.size()).isGreaterThan(1);
        assertThat(readRows(read, splits)).containsExactly("first", "second", "third", "fourth");
    }

    @Test
    void testCustomPathAndFormatAreIndependent() throws Exception {
        Path tablePath = new Path(tempDir.resolve("table").toUri());
        Path external = new Path(tempDir.resolve("external").toUri());
        writeFile(external, "archive", "orc", "external");
        Map<String, String> partitionOptions = options("orc");
        partitionOptions.put(CoreOptions.PATH.key(), new Path(external, "pt=archive").toString());
        FormatTable table =
                table(
                        tablePath,
                        "parquet",
                        Collections.singletonList(partition("archive", partitionOptions)));
        ReadBuilder read = table.newReadBuilder();
        assertThat(readRows(read, read.newScan().plan().splits())).containsExactly("external");
    }

    @ParameterizedTest
    @ValueSource(strings = {"static", "dynamic", "table"})
    void testAppendRejectsDifferentFormatAndOverwriteUpdatesFormat(String overwriteMode)
            throws Exception {
        Path location = new Path(tempDir.toUri());
        writeFile(location, "p", "orc", "old");
        FormatTable table =
                table(
                                location,
                                "parquet",
                                Collections.singletonList(partition("p", options("orc"))))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(),
                                        Boolean.toString(overwriteMode.equals("dynamic"))));
        BatchWriteBuilder append = table.newBatchWriteBuilder();
        List<CommitMessage> appendMessages;
        try (BatchTableWrite write = append.newWrite()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString("rejected"), BinaryString.fromString("p")));
            appendMessages = write.prepareCommit();
        }
        assertThatThrownBy(() -> append.newCommit().commit(appendMessages))
                .hasStackTraceContaining("registered with file.format=orc");
        assertThat(FormatTableScan.listDataFiles(table.fileIO(), new Path(location, "pt=p")))
                .hasSize(1);
        assertThat(
                        readRows(
                                table.newReadBuilder(),
                                table.newReadBuilder().newScan().plan().splits()))
                .containsExactly("old");

        AtomicReference<Map<String, String>> reported = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            List<Map<String, String>> options = invocation.getArgument(4);
                            if (options != null) {
                                reported.set(options.get(0));
                            }
                            return null;
                        })
                .when(table.partitionManager())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
        BatchWriteBuilder overwrite =
                table.newBatchWriteBuilder()
                        .withOverwrite(
                                overwriteMode.equals("static")
                                        ? Collections.singletonMap("pt", "p")
                                        : Collections.emptyMap());
        try (BatchTableWrite write = overwrite.newWrite()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString("replacement"), BinaryString.fromString("p")));
            overwrite.newCommit().commit(write.prepareCommit());
        }
        assertThat(reported.get()).containsEntry("file.format", "parquet");
        FormatTable reloaded =
                table(
                        location,
                        "parquet",
                        Collections.singletonList(
                                partition("p", options(reported.get().get("file.format")))));
        assertThat(
                        readRows(
                                reloaded.newReadBuilder(),
                                reloaded.newReadBuilder().newScan().plan().splits()))
                .containsExactly("replacement");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMatchingFormatWritesDoNotReportFormatOptions(boolean overwrite) throws Exception {
        Path location = new Path(tempDir.toUri());
        List<Partition> existing =
                Arrays.asList(
                        partition("inherited", null), partition("explicit", options("parquet")));
        FormatTable table = table(location, "parquet", existing);
        AtomicReference<List<Map<String, String>>> reported = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            reported.set(invocation.getArgument(4));
                            return null;
                        })
                .when(table.partitionManager())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        if (overwrite) {
            builder.withOverwrite(Collections.emptyMap());
        }
        try (BatchTableWrite write = builder.newWrite()) {
            for (String value : Arrays.asList("inherited", "explicit", "created")) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(value), BinaryString.fromString(value)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }
        if (overwrite) {
            assertThat(reported.get())
                    .hasSize(3)
                    .allSatisfy(options -> assertThat(options).doesNotContainKey("file.format"));
        } else {
            assertThat(reported.get()).isNull();
        }
        List<Partition> registered = new ArrayList<>(existing);
        registered.add(partition("created", null));
        ReadBuilder read = table(location, "parquet", registered).newReadBuilder();
        assertThat(readRows(read, read.newScan().plan().splits()))
                .containsExactlyInAnyOrder("inherited", "explicit", "created");
    }

    @ParameterizedTest
    @ValueSource(strings = {"static", "dynamic", "table"})
    void testEmptyOverwriteUpdatesOnlyTargetedFormats(String overwriteMode) throws Exception {
        Path location = new Path(tempDir.toUri());
        writeFile(location, "p", "orc", "old");
        FormatTable table =
                table(
                                location,
                                "parquet",
                                Collections.singletonList(partition("p", options("orc"))))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(),
                                        Boolean.toString(overwriteMode.equals("dynamic"))));
        AtomicReference<List<Map<String, String>>> reported = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            reported.set(invocation.getArgument(4));
                            return null;
                        })
                .when(table.partitionManager())
                .createPartitions(anyList(), anyBoolean(), any(), anyBoolean(), any());
        table.newBatchWriteBuilder()
                .withOverwrite(
                        overwriteMode.equals("static")
                                ? Collections.singletonMap("pt", "p")
                                : Collections.emptyMap())
                .newCommit()
                .commit(Collections.emptyList());

        if (overwriteMode.equals("dynamic")) {
            assertThat(reported.get()).isNull();
            ReadBuilder read = table.newReadBuilder();
            assertThat(readRows(read, read.newScan().plan().splits())).containsExactly("old");
        } else {
            assertThat(reported.get()).hasSize(1);
            assertThat(reported.get().get(0)).containsEntry("file.format", "parquet");
            assertThat(FormatTableScan.listDataFiles(table.fileIO(), new Path(location, "pt=p")))
                    .isEmpty();
        }
    }

    private FormatTable table(Path location, String format, List<Partition> partitions) {
        FormatTablePartitionManager manager = mock(FormatTablePartitionManager.class);
        when(manager.listPartitions(anyMap(), any())).thenReturn(partitions);
        when(manager.listPartitionsByNames(anyList())).thenReturn(partitions);
        Map<String, String> tableOptions = options(format);
        tableOptions.put(CoreOptions.PATH.key(), location.toString());
        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("db", "mixed"))
                .rowType(TABLE_TYPE)
                .partitionKeys(Collections.singletonList("pt"))
                .location(location.toString())
                .format(FormatTable.parseFormat(format))
                .catalogContext(CatalogContext.create(new Options()))
                .options(tableOptions)
                .partitionManager(manager)
                .build();
    }

    private static Partition partition(String value, Map<String, String> options) {
        return new Partition(
                Collections.singletonMap("pt", value),
                0,
                0,
                0,
                0,
                -1,
                false,
                null,
                null,
                null,
                null,
                options);
    }

    private static Map<String, String> options(String format) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), format);
        return options;
    }

    private static void writeFile(Path tablePath, String partition, String format, String... values)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tablePath, "pt=" + partition + "/data." + format);
        fileIO.mkdirs(path.getParent());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            FormatWriter writer =
                    FileFormat.fileFormat(new CoreOptions(options(format)))
                            .createWriterFactory(DATA_TYPE)
                            .create(out, "none");
            for (String value : values) {
                writer.addElement(GenericRow.of(BinaryString.fromString(value)));
            }
            writer.close();
        }
    }

    private static List<String> readRows(ReadBuilder builder, List<Split> splits) throws Exception {
        List<String> rows = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader =
                    builder.newRead().executeFilter().createReader(split)) {
                reader.forEachRemaining(row -> rows.add(row.getString(0).toString()));
            }
        }
        return rows;
    }
}
