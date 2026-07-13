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

package org.apache.paimon.eslib.index;

import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned metadata for an archived ESLib index. */
final class ESIndexFileMeta {

    private static final int MAGIC = 0x45534D31; // "ESM1"
    private static final int VERSION = 1;

    private ESIndexFileMeta() {}

    static byte[] write(File[] files, Map<String, FieldIndexConfig> fieldConfigs)
            throws IOException {
        if (files.length == 0) {
            return null;
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeInt(fieldConfigs.size());
        for (Map.Entry<String, FieldIndexConfig> entry : fieldConfigs.entrySet()) {
            FieldIndexConfig config = entry.getValue();
            writeString(out, entry.getKey());
            writeString(out, config.indexType().name());
            writeNullableEnum(out, config.algorithm());
            out.writeInt(config.dimension());
            writeNullableString(out, config.metric());
            writeNullableEnum(out, config.analyzer());
            writeNullableEnum(out, config.scalarType());
            out.writeInt(config.algorithmParams().size());
            for (Map.Entry<String, String> parameter : config.algorithmParams().entrySet()) {
                writeString(out, parameter.getKey());
                writeString(out, parameter.getValue());
            }
        }

        writeOffsets(out, files);
        out.flush();
        return bytes.toByteArray();
    }

    static Parsed read(byte[] metadata) throws IOException {
        if (metadata == null || metadata.length == 0) {
            return new Parsed(Collections.emptyMap(), Collections.emptyMap(), false);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(metadata));
        int first = in.readInt();
        if (first != MAGIC) {
            return new Parsed(Collections.emptyMap(), readOffsets(in, first), false);
        }

        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported es-index metadata version: " + version);
        }

        int configCount = readCount(in, "field config");
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        for (int i = 0; i < configCount; i++) {
            String fieldName = readString(in);
            FieldIndexConfig.IndexType indexType =
                    parseEnum(FieldIndexConfig.IndexType.class, readString(in), "index type");
            VectorAlgorithm algorithm = readNullableEnum(in, VectorAlgorithm.class, "algorithm");
            int dimension = in.readInt();
            String metric = readNullableString(in);
            BuiltinAnalyzer analyzer = readNullableEnum(in, BuiltinAnalyzer.class, "analyzer");
            ScalarFieldType scalarType = readNullableEnum(in, ScalarFieldType.class, "scalar type");

            int parameterCount = readCount(in, "algorithm parameter");
            Map<String, String> parameters = new LinkedHashMap<>();
            for (int j = 0; j < parameterCount; j++) {
                parameters.put(readString(in), readString(in));
            }

            FieldIndexConfig config =
                    FieldIndexConfig.builder(fieldName, indexType)
                            .algorithm(algorithm)
                            .dimension(dimension)
                            .metric(metric)
                            .analyzer(analyzer)
                            .scalarType(scalarType)
                            .algorithmParams(parameters)
                            .build();
            if (configs.put(fieldName, config) != null) {
                throw new IOException("Duplicate field config in es-index metadata: " + fieldName);
            }
        }

        int fileCount = readCount(in, "file");
        return new Parsed(configs, readOffsets(in, fileCount), true);
    }

    private static void writeOffsets(DataOutputStream out, File[] files) throws IOException {
        out.writeInt(files.length);
        long offset = 4;
        for (File file : files) {
            byte[] name = file.getName().getBytes(StandardCharsets.UTF_8);
            long length = file.length();
            long dataOffset = offset + 4 + name.length + 8;
            out.writeInt(name.length);
            out.write(name);
            out.writeLong(dataOffset);
            out.writeLong(length);
            offset = dataOffset + length;
        }
    }

    private static Map<String, long[]> readOffsets(DataInputStream in, int fileCount)
            throws IOException {
        if (fileCount < 0) {
            throw new IOException("Negative file count in es-index metadata: " + fileCount);
        }
        Map<String, long[]> offsets = new LinkedHashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String name = readString(in);
            long offset = in.readLong();
            long length = in.readLong();
            if (offset < 0 || length < 0) {
                throw new IOException("Negative file offset/length in es-index metadata: " + name);
            }
            offsets.put(name, new long[] {offset, length});
        }
        return offsets;
    }

    private static int readCount(DataInputStream in, String description) throws IOException {
        int count = in.readInt();
        if (count < 0) {
            throw new IOException(
                    "Negative " + description + " count in es-index metadata: " + count);
        }
        return count;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new EOFException("Invalid string length in es-index metadata: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    private static void writeNullableEnum(DataOutputStream out, Enum<?> value) throws IOException {
        writeNullableString(out, value == null ? null : value.name());
    }

    private static <E extends Enum<E>> E readNullableEnum(
            DataInputStream in, Class<E> enumClass, String description) throws IOException {
        String value = readNullableString(in);
        return value == null ? null : parseEnum(enumClass, value, description);
    }

    private static <E extends Enum<E>> E parseEnum(
            Class<E> enumClass, String value, String description) throws IOException {
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown " + description + " in es-index metadata: " + value, e);
        }
    }

    static final class Parsed {
        private final Map<String, FieldIndexConfig> fieldConfigs;
        private final Map<String, long[]> fileOffsets;
        private final boolean hasFieldConfigs;

        private Parsed(
                Map<String, FieldIndexConfig> fieldConfigs,
                Map<String, long[]> fileOffsets,
                boolean hasFieldConfigs) {
            this.fieldConfigs = fieldConfigs;
            this.fileOffsets = fileOffsets;
            this.hasFieldConfigs = hasFieldConfigs;
        }

        Map<String, FieldIndexConfig> fieldConfigs() {
            return fieldConfigs;
        }

        Map<String, long[]> fileOffsets() {
            return fileOffsets;
        }

        boolean hasFieldConfigs() {
            return hasFieldConfigs;
        }
    }
}
