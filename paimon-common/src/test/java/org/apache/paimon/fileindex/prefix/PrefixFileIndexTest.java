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

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/** Tests for {@link PrefixFileIndex}. */
public class PrefixFileIndexTest {

    @Test
    public void testStartsWith() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("hello");
        writer.write("world");
        writer.write("help");
        writer.write("helm");
        writer.write("helium");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // Existing prefixes should return REMAIN
        Assertions.assertThat(reader.visitStartsWith(null, "hel").remain()).isTrue();
        Assertions.assertThat(reader.visitStartsWith(null, "hello").remain()).isTrue();
        Assertions.assertThat(reader.visitStartsWith(null, "wor").remain()).isTrue();
        Assertions.assertThat(reader.visitStartsWith(null, "world").remain()).isTrue();
        Assertions.assertThat(reader.visitStartsWith(null, "he").remain()).isTrue();

        // Non-existing prefixes should return SKIP
        Assertions.assertThat(reader.visitStartsWith(null, "abc").remain()).isFalse();
        Assertions.assertThat(reader.visitStartsWith(null, "xyz").remain()).isFalse();
        // "helz" truncates to "hel" which exists in index, so it's a false positive (REMAIN)
        Assertions.assertThat(reader.visitStartsWith(null, "helz").remain()).isTrue();
    }

    @Test
    public void testEqual() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("apple");
        writer.write("apply");
        writer.write("banana");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // Equal to existing value: prefix matches, so REMAIN
        Assertions.assertThat(reader.visitEqual(null, "apple").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "apply").remain()).isTrue();

        // Equal to non-existing value with matching prefix: REMAIN (false positive)
        // "applx" has prefix "app" which exists in index
        Assertions.assertThat(reader.visitEqual(null, "applx").remain()).isTrue();

        // Equal to non-existing value with non-matching prefix: SKIP
        Assertions.assertThat(reader.visitEqual(null, "apricot").remain()).isFalse();
        Assertions.assertThat(reader.visitEqual(null, "ban").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "cherry").remain()).isFalse();
    }

    @Test
    public void testLikePrefixPattern() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("database");
        writer.write("dataflow");
        writer.write("datamine");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // "prefix%" pattern
        Assertions.assertThat(reader.visitLike(null, "dat%").remain()).isTrue();
        Assertions.assertThat(reader.visitLike(null, "data%").remain()).isTrue();
        Assertions.assertThat(reader.visitLike(null, "xyz%").remain()).isFalse();

        // Patterns with leading wildcard cannot use prefix index
        Assertions.assertThat(reader.visitLike(null, "%base").remain()).isTrue();
        Assertions.assertThat(reader.visitLike(null, "%ata%").remain()).isTrue();
    }

    @Test
    public void testNullValues() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("test");
        writer.write(null);
        writer.write("testing");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // IS NULL should return REMAIN when nulls exist
        Assertions.assertThat(reader.visitIsNull(null).remain()).isTrue();

        // STARTS_WITH with null literal should return REMAIN
        Assertions.assertThat(reader.visitStartsWith(null, null).remain()).isTrue();
    }

    @Test
    public void testNoNullValues() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("only");
        writer.write("values");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // IS NULL should return SKIP when no nulls exist
        Assertions.assertThat(reader.visitIsNull(null).remain()).isFalse();
    }

    @Test
    public void testCustomPrefixLength() {
        PrefixFileIndex index =
                new PrefixFileIndex(
                        DataTypes.STRING(),
                        new Options(
                                new HashMap<String, String>() {
                                    {
                                        put("prefix-length", "2");
                                    }
                                }));
        FileIndexWriter writer = index.createWriter();

        writer.write("abcde");
        writer.write("abxyz");
        writer.write("bcdef");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // With prefix-length=2, "ab" matches both "abcde" and "abxyz"
        Assertions.assertThat(reader.visitStartsWith(null, "ab").remain()).isTrue();
        Assertions.assertThat(reader.visitStartsWith(null, "abc").remain()).isTrue();

        // "bc" matches "bcdef"
        Assertions.assertThat(reader.visitStartsWith(null, "bc").remain()).isTrue();

        // "xy" does not match any prefix
        Assertions.assertThat(reader.visitStartsWith(null, "xy").remain()).isFalse();
    }

    @Test
    public void testShortValues() {
        PrefixFileIndex index =
                new PrefixFileIndex(DataTypes.STRING(), new Options(new HashMap<String, String>()));
        FileIndexWriter writer = index.createWriter();

        writer.write("ab");
        writer.write("a");
        writer.write("abc");

        byte[] serialized = writer.serializedBytes();
        FileIndexReader reader =
                index.createReader(new ByteArraySeekableStream(serialized), 0, serialized.length);

        // "ab" is both a prefix and a full value
        Assertions.assertThat(reader.visitStartsWith(null, "ab").remain()).isTrue();
        // "a" matches "a", "ab", "abc"
        Assertions.assertThat(reader.visitStartsWith(null, "a").remain()).isTrue();
    }
}
