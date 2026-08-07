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
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NgramFileIndexTest {

    @Test
    void testStartsWithShouldSkip() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));
        writer.write(BinaryString.fromString("world"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitStartsWith(null, BinaryString.fromString("he")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("wo")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("xyz")).remain()).isFalse();
    }

    @Test
    void testEndsWithShouldSkip() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));
        writer.write(BinaryString.fromString("world"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitEndsWith(null, BinaryString.fromString("lo")).remain()).isTrue();
        assertThat(reader.visitEndsWith(null, BinaryString.fromString("ld")).remain()).isTrue();
        assertThat(reader.visitEndsWith(null, BinaryString.fromString("xyz")).remain()).isFalse();
    }

    @Test
    void testContainsShouldSkip() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));
        writer.write(BinaryString.fromString("world"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitContains(null, BinaryString.fromString("ll")).remain()).isTrue();
        assertThat(reader.visitContains(null, BinaryString.fromString("or")).remain()).isTrue();
        assertThat(reader.visitContains(null, BinaryString.fromString("xyz")).remain()).isFalse();
    }

    @Test
    void testEqualShouldWork() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitEqual(null, BinaryString.fromString("hello")).remain()).isTrue();
        assertThat(reader.visitEqual(null, BinaryString.fromString("world")).remain()).isFalse();
    }

    @Test
    void testShortPatternReturnsRemain() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitStartsWith(null, BinaryString.fromString("h")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("a")).remain()).isTrue();
    }

    @Test
    void testDifferentGramSize() {
        Options options = new Options();
        options.set("gram_size", "3");
        NgramFileIndex index = new NgramFileIndex(options);
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitStartsWith(null, BinaryString.fromString("hel")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("xyz")).remain()).isFalse();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("he")).remain()).isTrue();
    }

    @Test
    void testSerializationDeserialization() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("apple"));
        writer.write(BinaryString.fromString("application"));
        writer.write(BinaryString.fromString("banana"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitStartsWith(null, BinaryString.fromString("ap")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("ba")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("xyz")).remain()).isFalse();
    }

    @Test
    void testNullValueHandling() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));
        writer.write(null);
        writer.write(BinaryString.fromString("world"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitStartsWith(null, BinaryString.fromString("he")).remain()).isTrue();
        assertThat(reader.visitStartsWith(null, BinaryString.fromString("xyz")).remain()).isFalse();
    }

    @Test
    void testLikePattern() {
        NgramFileIndex index = new NgramFileIndex(new Options());
        FileIndexWriter writer = index.createWriter();
        writer.write(BinaryString.fromString("hello"));
        writer.write(BinaryString.fromString("world"));

        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader = createReader(index, bytes);

        assertThat(reader.visitLike(null, BinaryString.fromString("h%o")).remain()).isTrue();
        assertThat(reader.visitLike(null, BinaryString.fromString("w%d")).remain()).isTrue();
    }

    @Test
    void testNonStringTypeThrowsException() {
        NgramFileIndexFactory factory = new NgramFileIndexFactory();
        assertThatThrownBy(() -> factory.create(DataTypes.INT(), new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("N-gram index only supports string types");
    }

    private FileIndexReader createReader(NgramFileIndex index, byte[] bytes) {
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        return index.createReader(stream, 0, bytes.length);
    }
}
