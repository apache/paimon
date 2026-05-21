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

package org.apache.paimon.fileindex.ngram;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.utils.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/** N-gram file index for string prefix/suffix/contains queries. */
public class NgramFileIndex implements FileIndexer {

    private static final int DEFAULT_GRAM_SIZE = 2;
    private static final String GRAM_SIZE = "gram_size";

    private final int gramSize;

    public NgramFileIndex(Options options) {
        this.gramSize = options.getInteger(GRAM_SIZE, DEFAULT_GRAM_SIZE);
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(gramSize);
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

    /** Writer for N-gram index. */
    private static class Writer extends FileIndexWriter {

        private final int gramSize;
        private final Set<String> ngramSet = new HashSet<>();

        Writer(int gramSize) {
            this.gramSize = gramSize;
        }

        @Override
        public void write(Object key) {
            if (key == null) {
                return;
            }
            String value = ((BinaryString) key).toString();
            addNgrams(value);
        }

        private void addNgrams(String value) {
            if (value.length() < gramSize) {
                ngramSet.add(value);
            } else {
                for (int i = 0; i <= value.length() - gramSize; i++) {
                    ngramSet.add(value.substring(i, i + gramSize));
                }
            }
        }

        @Override
        public byte[] serializedBytes() {
            try {
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream dataOut = new DataOutputStream(byteOut);

                dataOut.writeInt(gramSize);
                dataOut.writeInt(ngramSet.size());

                for (String token : ngramSet) {
                    byte[] tokenBytes = token.getBytes("UTF-8");
                    dataOut.writeShort(tokenBytes.length);
                    dataOut.write(tokenBytes);
                }

                dataOut.close();
                return byteOut.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Reader for N-gram index. */
    private static class Reader extends FileIndexReader {

        private final int gramSize;
        private final Set<String> ngramSet;

        Reader(byte[] serializedBytes) {
            try {
                DataInputStream dataIn =
                        new DataInputStream(new java.io.ByteArrayInputStream(serializedBytes));
                this.gramSize = dataIn.readInt();
                int setSize = dataIn.readInt();
                this.ngramSet = new HashSet<>(setSize);

                for (int i = 0; i < setSize; i++) {
                    int tokenLength = dataIn.readShort();
                    byte[] tokenBytes = new byte[tokenLength];
                    dataIn.readFully(tokenBytes);
                    ngramSet.add(new String(tokenBytes, "UTF-8"));
                }

                dataIn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return checkPattern(literalToString(literal));
        }

        @Override
        public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
            return checkPattern(literalToString(literal));
        }

        @Override
        public FileIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
            return checkPattern(literalToString(literal));
        }

        @Override
        public FileIndexResult visitContains(FieldRef fieldRef, Object literal) {
            return checkPattern(literalToString(literal));
        }

        @Override
        public FileIndexResult visitLike(FieldRef fieldRef, Object literal) {
            String pattern = literalToString(literal);
            String[] parts = pattern.split("%");
            String longestPart = "";
            for (String part : parts) {
                if (part.length() > longestPart.length()) {
                    longestPart = part;
                }
            }
            return checkPattern(longestPart);
        }

        private FileIndexResult checkPattern(String pattern) {
            if (pattern == null || pattern.isEmpty() || pattern.length() < gramSize) {
                return REMAIN;
            }

            for (int i = 0; i <= pattern.length() - gramSize; i++) {
                String ngram = pattern.substring(i, i + gramSize);
                if (!ngramSet.contains(ngram)) {
                    return SKIP;
                }
            }
            return REMAIN;
        }

        private String literalToString(Object literal) {
            if (literal == null) {
                return null;
            }
            if (literal instanceof BinaryString) {
                return ((BinaryString) literal).toString();
            }
            return literal.toString();
        }
    }
}
