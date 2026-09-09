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

package org.apache.paimon.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowType#collectLeafPaths} and {@link RowType#projectByPaths}. */
class RowTypeTest {

    @Test
    void collectLeafPathsRejectsDottedPathCollidingWithTopLevelFieldName() {
        // fullType has a top-level field literally named "a.b" (id 5), plus a struct "a" (id 6)
        // with children x (id 7) and b (id 8).
        RowType fullType =
                new RowType(
                        Arrays.asList(
                                new DataField(5, "a.b", new IntType()),
                                new DataField(
                                        6,
                                        "a",
                                        new RowType(
                                                Arrays.asList(
                                                        new DataField(7, "x", new IntType()),
                                                        new DataField(8, "b", new IntType()))))));

        // Partial write: only the "b" child of struct "a" is written (id 8), not "x". Its
        // flattened dotted path "a.b" would collide with the literal top-level field of the same
        // name.
        RowType writeType =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        6,
                                        "a",
                                        new RowType(
                                                Arrays.asList(
                                                        new DataField(8, "b", new IntType()))))));

        assertThatThrownBy(() -> writeType.collectLeafPaths(fullType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("a.b");
    }

    @Test
    void collectLeafPathsPreservesReorderedFullStructWriteOrder() {
        // fullType: nest<a INT, b STRING> declared in order a, b.
        RowType nestFull =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "a", new IntType()),
                                new DataField(3, "b", VarCharType.STRING_TYPE)));
        RowType fullType = new RowType(Arrays.asList(new DataField(1, "nest", nestFull)));

        // Physically written in reversed order: nest<b, a>.
        RowType writeType = fullType.projectByPaths(Arrays.asList("nest.b", "nest.a"));
        RowType writtenNest = (RowType) writeType.getFields().get(0).type();
        assertThat(writtenNest.getFieldNames()).containsExactly("b", "a");

        // Even though every sub-field of "nest" is present, the reordered layout must not
        // collapse to the bare top-level name "nest" (that would silently discard the physical
        // write order and let a reader reconstruct schema-declaration order instead).
        List<String> leafPaths = writeType.collectLeafPaths(fullType);
        assertThat(leafPaths).containsExactly("nest.b", "nest.a");

        RowType reconstructed = fullType.projectByPaths(leafPaths);
        RowType reconstructedNest = (RowType) reconstructed.getFields().get(0).type();
        assertThat(reconstructedNest.getFieldNames()).isEqualTo(writtenNest.getFieldNames());
    }

    @Test
    void collectLeafPathsCollapsesToWholeFieldWhenOrderMatches() {
        // Same schema, but written in declaration order: coversFully should still collapse to
        // the bare top-level name, since nothing is ambiguous or reordered here.
        RowType nestFull =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "a", new IntType()),
                                new DataField(3, "b", VarCharType.STRING_TYPE)));
        RowType fullType = new RowType(Arrays.asList(new DataField(1, "nest", nestFull)));

        RowType writeType = fullType.projectByPaths(Arrays.asList("nest.a", "nest.b"));
        assertThat(writeType.collectLeafPaths(fullType)).containsExactly("nest");
    }
}
