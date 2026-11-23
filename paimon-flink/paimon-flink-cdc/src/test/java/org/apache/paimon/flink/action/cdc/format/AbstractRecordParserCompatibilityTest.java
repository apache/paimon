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

package org.apache.paimon.flink.action.cdc.format;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.kafka.KafkaMetadataConverter;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Binary compatibility of the protected helpers used by custom CDC format parsers. */
class AbstractRecordParserCompatibilityTest {

    @TempDir private Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(strings = {"evalComputedColumns", "evalMetadataColumns"})
    void testLegacyParserHelper(String helperName) throws Exception {
        Path classesDirectory = compileLegacyParser(helperName);
        URL[] urls = {classesDirectory.toUri().toURL()};
        // Resolve the superclass from the running connector, not the compilation stub.
        try (URLClassLoader classLoader =
                new URLClassLoader(urls, AbstractRecordParser.class.getClassLoader())) {
            Class<?> legacyClass =
                    classLoader.loadClass(
                            "org.apache.paimon.flink.action.cdc.format.legacy.LegacyRecordParser");
            assertThat(legacyClass.getSuperclass()).isSameAs(AbstractRecordParser.class);
            List<ComputedColumn> columns =
                    buildComputedColumns(
                            Collections.singletonList("derived=substring(id,0)"),
                            Collections.singletonList(new DataField(0, "id", DataTypes.STRING())));
            AbstractRecordParser parser =
                    (AbstractRecordParser)
                            legacyClass.getConstructor(List.class).newInstance(columns);
            parser.withMetadataConverters(
                    new CdcMetadataConverter[] {new KafkaMetadataConverter.TopicConverter()});
            List<RichCdcMultiplexRecord> records = new ArrayList<>();

            assertThatCode(
                            () ->
                                    parser.flatMap(
                                            new CdcSourceRecord("legacy-topic", null, "unused"),
                                            new ListCollector<>(records)))
                    .as("Legacy parsers must still be able to call %s", helperName)
                    .doesNotThrowAnyException();

            boolean computed = helperName.equals("evalComputedColumns");
            String outputColumn = computed ? "derived" : "topic";
            assertThat(records).hasSize(1);
            assertThat(records.get(0).toRichCdcRecord().toCdcRecord().data())
                    .hasSize(2)
                    .containsEntry("id", "source-value")
                    .containsEntry(outputColumn, computed ? "source-value" : "legacy-topic");
            assertThat(records.get(0).cdcSchema().fields())
                    .extracting(DataField::name)
                    .containsExactly("id", outputColumn);
        }
    }

    private Path compileLegacyParser(String helperName) throws Exception {
        Path sourceDirectory = temporaryDirectory.resolve("source");
        Path classesDirectory = temporaryDirectory.resolve("classes");
        Path legacyBase =
                sourceDirectory.resolve(
                        "org/apache/paimon/flink/action/cdc/format/AbstractRecordParser.java");
        Path legacyImplementation =
                sourceDirectory.resolve(
                        "org/apache/paimon/flink/action/cdc/format/legacy/LegacyRecordParser.java");
        Files.createDirectories(legacyBase.getParent());
        Files.createDirectories(legacyImplementation.getParent());
        Files.createDirectories(classesDirectory);

        // Only the old superclass API is needed to compile a downstream parser binary.
        Files.write(
                legacyBase,
                Arrays.asList(
                        "package org.apache.paimon.flink.action.cdc.format;",
                        "import java.util.List;",
                        "import java.util.Map;",
                        "import org.apache.paimon.flink.action.cdc.ComputedColumn;",
                        "import org.apache.paimon.flink.action.cdc.TypeMapping;",
                        "import org.apache.paimon.flink.sink.cdc.CdcSchema;",
                        "import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;",
                        "import org.apache.paimon.types.RowKind;",
                        "public abstract class AbstractRecordParser {",
                        "  public AbstractRecordParser(TypeMapping mapping, List<ComputedColumn> columns) {}",
                        "  protected void evalComputedColumns(Map<String, String> row, CdcSchema.Builder schema) {}",
                        "  protected void evalMetadataColumns(Map<String, String> row, CdcSchema.Builder schema) {}",
                        "  protected abstract List<RichCdcMultiplexRecord> extractRecords();",
                        "  protected abstract List<String> extractPrimaryKeys();",
                        "  protected abstract String getTableName();",
                        "  protected abstract String getDatabaseName();",
                        "  protected abstract String format();",
                        "  protected RichCdcMultiplexRecord createRecord(",
                        "      RowKind kind, Map<String, String> row, CdcSchema.Builder schema) { return null; }",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                legacyImplementation,
                Arrays.asList(
                        "package org.apache.paimon.flink.action.cdc.format.legacy;",
                        "import java.util.Collections;",
                        "import java.util.LinkedHashMap;",
                        "import java.util.List;",
                        "import java.util.Map;",
                        "import org.apache.paimon.flink.action.cdc.ComputedColumn;",
                        "import org.apache.paimon.flink.action.cdc.TypeMapping;",
                        "import org.apache.paimon.flink.action.cdc.format.AbstractRecordParser;",
                        "import org.apache.paimon.flink.sink.cdc.CdcSchema;",
                        "import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;",
                        "import org.apache.paimon.types.DataTypes;",
                        "import org.apache.paimon.types.RowKind;",
                        "public class LegacyRecordParser extends AbstractRecordParser {",
                        "  public LegacyRecordParser(List<ComputedColumn> columns) {",
                        "    super(TypeMapping.defaultMapping(), columns);",
                        "  }",
                        "  @Override protected List<RichCdcMultiplexRecord> extractRecords() {",
                        "    Map<String, String> row = new LinkedHashMap<>();",
                        "    row.put(\"id\", \"source-value\");",
                        "    CdcSchema.Builder schema = CdcSchema.newBuilder();",
                        "    schema.column(\"id\", DataTypes.STRING());",
                        "    " + helperName + "(row, schema);",
                        "    return Collections.singletonList(createRecord(RowKind.INSERT, row, schema));",
                        "  }",
                        "  @Override protected List<String> extractPrimaryKeys() { return Collections.emptyList(); }",
                        "  @Override protected String getTableName() { return \"records\"; }",
                        "  @Override protected String getDatabaseName() { return \"legacy\"; }",
                        "  @Override protected String format() { return \"legacy\"; }",
                        "}"),
                StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("A JDK compiler is required").isNotNull();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            boolean compiled =
                    compiler.getTask(
                                    null,
                                    fileManager,
                                    diagnostics,
                                    Arrays.asList(
                                            "-classpath",
                                            System.getProperty("java.class.path"),
                                            "-d",
                                            classesDirectory.toString(),
                                            "-source",
                                            "8",
                                            "-target",
                                            "8",
                                            "-proc:none",
                                            "-Xlint:-options"),
                                    null,
                                    fileManager.getJavaFileObjects(
                                            legacyBase.toFile(), legacyImplementation.toFile()))
                            .call();
            assertThat(compiled)
                    .as("Legacy fixture compilation: %s", diagnostics.getDiagnostics())
                    .isTrue();
        }
        return classesDirectory;
    }
}
