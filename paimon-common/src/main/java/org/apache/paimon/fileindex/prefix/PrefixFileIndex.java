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

package org.apache.paimon.fileindex.prefix;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Prefix file index for accelerating prefix match queries (e.g. LIKE 'prefix%', STARTS_WITH).
 *
 * <p>For each text value, extracts a fixed-length prefix (default 3 characters) and builds an
 * inverted index: prefix -&gt; RoaringBitmap of row numbers. Queries with a prefix literal can
 * quickly determine whether the data file needs to be read.
 */
public class PrefixFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;
    public static final String VERSION = "version";
    public static final String PREFIX_LENGTH = "prefix-length";

    private static final int DEFAULT_PREFIX_LENGTH = 3;

    private final DataType dataType;
    private final Options options;

    public PrefixFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(options);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            byte[] serializedBytes = new byte[length];
            IOUtils.readFully(inputStream, serializedBytes);
            return new Reader(serializedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String keyToString(Object key) {
        if (key instanceof BinaryString) {
            return key.toString();
        } else if (key instanceof String) {
            return (String) key;
        }
        throw new IllegalArgumentException(
                "Prefix index only supports string types, but got: "
                        + (key == null ? "null" : key.getClass().getName()));
    }

    private static String extractPrefix(String text, int prefixLength) {
        if (text.length() <= prefixLength) {
            return text;
        }
        return text.substring(0, prefixLength);
    }

    // ==================== Writer ====================

    private static class Writer extends FileIndexWriter {

        private final int prefixLength;
        private final Map<String, RoaringBitmap32> prefix2bitmap = new HashMap<>();
        private final RoaringBitmap32 nullBitmap = new RoaringBitmap32();
        private int rowNumber;

        Writer(Options options) {
            this.prefixLength = options.getInteger(PREFIX_LENGTH, DEFAULT_PREFIX_LENGTH);
            if (prefixLength <= 0) {
                throw new IllegalArgumentException(
                        "prefix-length must be positive, but got: " + prefixLength);
            }
        }

        @Override
        public void write(Object key) {
            if (key == null) {
                nullBitmap.add(rowNumber++);
            } else {
                String prefix = extractPrefix(keyToString(key), prefixLength);
                prefix2bitmap.computeIfAbsent(prefix, k -> new RoaringBitmap32()).add(rowNumber++);
            }
        }

        @Override
        public byte[] serializedBytes() {
            try {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(output);

                // Compute body layout first
                byte[] nullBitmapBytes = null;
                int nullOffset;
                int bodyOffset = 0;

                if (nullBitmap.isEmpty()) {
                    nullOffset = 0;
                } else if (nullBitmap.getCardinality() == 1) {
                    nullOffset = -1 - nullBitmap.first();
                } else {
                    nullBitmapBytes = serializeBitmap(nullBitmap);
                    nullOffset = 0; // null bitmap at start of body
                    bodyOffset = nullBitmapBytes.length;
                }

                // Sort entries by prefix for deterministic serialization
                LinkedHashMap<String, Integer> offsets = new LinkedHashMap<>();
                LinkedHashMap<String, byte[]> bitmapBytes = new LinkedHashMap<>();

                for (String prefix : sortedPrefixes()) {
                    RoaringBitmap32 bitmap = prefix2bitmap.get(prefix);
                    byte[] bytes = serializeBitmap(bitmap);
                    offsets.put(prefix, bodyOffset);
                    bitmapBytes.put(prefix, bytes);
                    bodyOffset += bytes.length;
                }

                // Write header
                dos.writeByte(VERSION_1);
                dos.writeInt(prefixLength);
                dos.writeInt(rowNumber);
                dos.writeInt(prefix2bitmap.size());
                dos.writeBoolean(!nullBitmap.isEmpty());
                dos.writeInt(nullOffset);

                // Write entries (prefix + offset)
                for (Map.Entry<String, Integer> entry : offsets.entrySet()) {
                    byte[] prefixBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(prefixBytes.length);
                    dos.write(prefixBytes);
                    dos.writeInt(entry.getValue());
                }

                // Write bitmap body
                if (nullBitmapBytes != null) {
                    dos.write(nullBitmapBytes);
                }
                for (byte[] bytes : bitmapBytes.values()) {
                    dos.write(bytes);
                }

                dos.flush();
                return output.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize prefix file index", e);
            }
        }

        private byte[] serializeBitmap(RoaringBitmap32 bitmap) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            bitmap.serialize(dos);
            dos.flush();
            return baos.toByteArray();
        }

        private java.util.List<String> sortedPrefixes() {
            java.util.List<String> list = new java.util.ArrayList<>(prefix2bitmap.keySet());
            java.util.Collections.sort(list);
            return list;
        }
    }

    // ==================== Reader ====================

    private static class Reader extends FileIndexReader {

        private final byte[] data;
        private int prefixLength;

        // Lazy loaded
        private int rowCount;
        private boolean hasNull;
        private int nullOffset;
        private Map<String, Integer> prefixOffsets;
        private int bodyStart;

        Reader(byte[] data) {
            this.data = data;
            // prefixLength is not stored in serialized data; we use a reasonable default
            // In practice, query prefix extraction should match writer's prefixLength.
            // For simplicity, we use the default here; the query literal's prefix is extracted
            // with the same logic as the writer (min of literal length and prefix length).
            this.prefixLength = DEFAULT_PREFIX_LENGTH;
        }

        private void ensureLoaded() {
            if (prefixOffsets != null) {
                return;
            }
            try {
                java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
                DataInputStream dis = new DataInputStream(bais);

                int version = dis.readByte();
                if (version != VERSION_1) {
                    throw new RuntimeException("Unsupported prefix file index version: " + version);
                }
                prefixLength = dis.readInt();
                rowCount = dis.readInt();
                int entryCount = dis.readInt();
                hasNull = dis.readBoolean();
                nullOffset = dis.readInt();

                prefixOffsets = new HashMap<>(entryCount);
                for (int i = 0; i < entryCount; i++) {
                    int prefixLen = dis.readInt();
                    byte[] prefixBytes = new byte[prefixLen];
                    dis.readFully(prefixBytes);
                    String prefix = new String(prefixBytes, StandardCharsets.UTF_8);
                    int offset = dis.readInt();
                    prefixOffsets.put(prefix, offset);
                }
                bodyStart = data.length - bais.available();
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize prefix file index", e);
            }
        }

        private RoaringBitmap32 readBitmap(int offset) {
            try {
                java.io.ByteArrayInputStream bais =
                        new java.io.ByteArrayInputStream(
                                data, bodyStart + offset, data.length - bodyStart - offset);
                DataInputStream dis = new DataInputStream(bais);
                RoaringBitmap32 bitmap = new RoaringBitmap32();
                bitmap.deserialize(dis);
                return bitmap;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read bitmap from prefix file index", e);
            }
        }

        private boolean hasPrefix(String prefix) {
            ensureLoaded();
            Integer offset = prefixOffsets.get(prefix);
            if (offset != null) {
                if (offset < 0) {
                    // single value shortcut
                    return true;
                }
                RoaringBitmap32 bitmap = readBitmap(offset);
                return !bitmap.isEmpty();
            }
            // If exact prefix not found, check if any stored prefix starts with the query prefix.
            // This handles the case where query prefix is shorter than prefixLength.
            for (Map.Entry<String, Integer> entry : prefixOffsets.entrySet()) {
                if (entry.getKey().startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
            if (literal == null) {
                return FileIndexResult.REMAIN;
            }
            String text = keyToString(literal);
            String prefix = extractPrefix(text, prefixLength);
            return hasPrefix(prefix) ? FileIndexResult.REMAIN : FileIndexResult.SKIP;
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            if (literal == null) {
                return hasNull ? FileIndexResult.REMAIN : FileIndexResult.SKIP;
            }
            String text = keyToString(literal);
            String prefix = extractPrefix(text, prefixLength);
            return hasPrefix(prefix) ? FileIndexResult.REMAIN : FileIndexResult.SKIP;
        }

        @Override
        public FileIndexResult visitLike(FieldRef fieldRef, Object literal) {
            if (literal == null) {
                return FileIndexResult.REMAIN;
            }
            String pattern = keyToString(literal);
            // Optimize for "prefix%" patterns (no leading wildcard, single trailing %)
            if (pattern.endsWith("%")
                    && !pattern.startsWith("%")
                    && pattern.indexOf('%') == pattern.length() - 1
                    && pattern.indexOf('_') == -1) {
                String prefixText = pattern.substring(0, pattern.length() - 1);
                String prefix = extractPrefix(prefixText, prefixLength);
                return hasPrefix(prefix) ? FileIndexResult.REMAIN : FileIndexResult.SKIP;
            }
            return FileIndexResult.REMAIN;
        }

        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            ensureLoaded();
            return hasNull ? FileIndexResult.REMAIN : FileIndexResult.SKIP;
        }
    }
}
