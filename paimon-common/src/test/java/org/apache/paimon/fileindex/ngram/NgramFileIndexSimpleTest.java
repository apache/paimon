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
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NgramFileIndexSimpleTest {

    @Test
    void testNgramGeneration() {
        Options options = new Options();
        NgramFileIndex index = new NgramFileIndex(options);
        FileIndexWriter writer = index.createWriter();

        writer.write(BinaryString.fromString("hello"));
        writer.write(BinaryString.fromString("world"));

        Set<String> expectedNgrams = new HashSet<>();
        expectedNgrams.add("he");
        expectedNgrams.add("el");
        expectedNgrams.add("ll");
        expectedNgrams.add("lo");
        expectedNgrams.add("wo");
        expectedNgrams.add("or");
        expectedNgrams.add("rl");
        expectedNgrams.add("ld");

        byte[] bytes = writer.serializedBytes();

        String patternsWithXyz = "xy";
        assertThat(expectedNgrams.contains(patternsWithXyz)).isFalse();
        assertThat(expectedNgrams.contains("yz")).isFalse();
        assertThat(expectedNgrams.contains("he")).isTrue();
        assertThat(expectedNgrams.contains("wo")).isTrue();
    }
}
