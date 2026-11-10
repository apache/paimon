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

package org.apache.paimon.flink.action.cdc.serialization;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcDebeziumDeserializationSchema}. */
class CdcDebeziumDeserializationSchemaTest {

    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"UTF-8", "US-ASCII"})
    void testPreserveJsonText(String charset) throws Exception {
        Path output = tempDir.resolve("output.log");
        // The default charset is cached, so configure it when starting a fresh JVM.
        Process process =
                new ProcessBuilder(
                                new File(System.getProperty("java.home"), "bin/java")
                                        .getAbsolutePath(),
                                "-Dfile.encoding=" + charset,
                                "-cp",
                                createClasspathJar().toString(),
                                CharsetCheck.class.getName(),
                                charset)
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertThat(process.waitFor(60, TimeUnit.SECONDS))
                    .as("Charset check process completed")
                    .isTrue();
            assertThat(process.exitValue())
                    .withFailMessage(
                            "Charset check (%s) failed:%n%s",
                            charset, new String(Files.readAllBytes(output), StandardCharsets.UTF_8))
                    .isZero();
        } finally {
            process.destroyForcibly();
        }
    }

    private Path createClasspathJar() throws IOException {
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        StringJoiner entries = new StringJoiner(" ");
        for (String entry : classpath.split(File.pathSeparator)) {
            entries.add(new File(entry).toURI().toASCIIString());
        }

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, entries.toString());
        Path classpathJar = tempDir.resolve("classpath.jar");
        // Keep the child JVM command line below the Windows length limit.
        try (JarOutputStream ignored =
                new JarOutputStream(Files.newOutputStream(classpathJar), manifest)) {
            // The manifest references the original classpath; no class entries are needed.
        }
        return classpathJar;
    }

    /** Entry point for the JVM with an explicitly configured default charset. */
    public static class CharsetCheck {

        public static void main(String[] args) throws Exception {
            // Preserve assertion diagnostics without changing the default charset under test.
            System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8.name()));
            assertThat(Charset.defaultCharset()).isEqualTo(Charset.forName(args[0]));
            String text = "ASCII 中文😀\"\\\n";
            Schema schema = SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();
            SourceRecord input =
                    new SourceRecord(
                            null, null, "topic", schema, new Struct(schema).put("text", text));
            for (boolean includeSchema : new boolean[] {false, true}) {
                List<CdcSourceRecord> results = new ArrayList<>();
                new CdcDebeziumDeserializationSchema(includeSchema)
                        .deserialize(input, new ListCollector<>(results));
                assertThat(results).hasSize(1);
                JsonNode actual = new ObjectMapper().readTree((String) results.get(0).getValue());
                if (includeSchema) {
                    actual = actual.get("payload");
                }
                assertThat(actual.get("text").asText()).isEqualTo(text);
            }
        }
    }
}
