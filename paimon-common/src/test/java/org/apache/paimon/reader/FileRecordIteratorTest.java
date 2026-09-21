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

package org.apache.paimon.reader;

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileRecordIterator}. */
public class FileRecordIteratorTest {

    @Test
    public void testSelection() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);
        selection.add(5);
        selection.add(7);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(1L, 3L, 5L, 7L);
    }

    @Test
    public void testSelectionWithEmptySelection() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).isEmpty();
    }

    @Test
    public void testSelectionWithAllSelected() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        for (int i = 0; i < values.size(); i++) {
            selection.add(i);
        }

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    public void testSelectionWithFirstElement() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(0L);
    }

    @Test
    public void testSelectionWithLastElement() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(4);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(4L);
    }

    @Test
    public void testSelectionWithConsecutivePositions() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(2);
        selection.add(3);
        selection.add(4);
        selection.add(5);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(2L, 3L, 4L, 5L);
    }

    @Test
    public void testSelectionWithSparsePositions() throws IOException {
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i);
        }
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(10);
        selection.add(50);
        selection.add(99);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(0L, 10L, 50L, 99L);
    }

    @Test
    public void testSelectionWithOutOfRangePositions() throws IOException {
        List<Long> values = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);
        selection.add(10);
        selection.add(20);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        List<Long> result = collectAll(filteredIterator);

        assertThat(result).containsExactly(1L, 3L);
    }

    @Test
    public void testSelectionPositionTracking() throws IOException {
        List<Long> values = Arrays.asList(10L, 20L, 30L, 40L, 50L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        Long first = filteredIterator.next();
        assertThat(first).isEqualTo(20L);
        assertThat(filteredIterator.returnedPosition()).isEqualTo(1);

        Long second = filteredIterator.next();
        assertThat(second).isEqualTo(40L);
        assertThat(filteredIterator.returnedPosition()).isEqualTo(3);

        Long third = filteredIterator.next();
        assertThat(third).isNull();
    }

    @Test
    public void testSelectionSkip() throws IOException {
        int[] materialized = {0};
        FileRecordIterator<Long> iterator =
                createIterator(Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L))
                        .transform(
                                value -> {
                                    materialized[0]++;
                                    return value;
                                });

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(3);
        selection.add(5);

        FileRecordIterator<Long> selected = iterator.selection(selection);
        assertThat(selected.skip()).isTrue();
        assertThat(selected.returnedPosition()).isEqualTo(1L);
        assertThat(materialized[0]).isZero();
        assertThat(selected.next()).isEqualTo(3L);
        assertThat(selected.returnedPosition()).isEqualTo(3L);
        assertThat(selected.skip()).isTrue();
        assertThat(selected.returnedPosition()).isEqualTo(5L);
        assertThat(selected.skip()).isFalse();
        assertThat(materialized[0]).isEqualTo(2);
    }

    @Test
    public void testSelectionSeeksWithoutWalkingPrefixesOrGaps() throws IOException {
        int[] accesses = {0};
        RoaringBitmap32 selection =
                new RoaringBitmap32() {
                    @Override
                    public long nextValue(int fromValue) {
                        accesses[0]++;
                        return super.nextValue(fromValue);
                    }

                    @Override
                    public Iterator<Integer> iterator() {
                        Iterator<Integer> iterator = super.iterator();
                        return new Iterator<Integer>() {
                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public Integer next() {
                                accesses[0]++;
                                return iterator.next();
                            }
                        };
                    }
                };
        selection.flip(0, 100_000);
        FileRecordIterator<Long> selected =
                createPositionIterator(Arrays.asList(99_990L, 99_991L, 99_999L), new int[1])
                        .selection(selection);
        assertThat(collectAll(selected)).containsExactly(99_990L, 99_991L, 99_999L);
        assertThat(accesses[0]).isLessThanOrEqualTo(4);

        selection.clear();
        selection.add(0);
        selection.add(100_000);
        accesses[0] = 0;
        List<Long> positions = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            positions.add(i);
        }
        int[] releases = {0};
        selected = createPositionIterator(positions, releases).selection(selection);
        assertThat(selected.skip()).isTrue();
        assertThat(selected.returnedPosition()).isZero();
        assertThat(selected.next()).isNull();
        assertThat(accesses[0]).isLessThanOrEqualTo(3);
        selected.releaseBatch();
        assertThat(releases[0]).isOne();
    }

    @Test
    public void testSelectionAcrossBatchesAndPositionGaps() throws IOException {
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.flip(0, 100_000);
        selection.remove(99_992);
        selection.remove(99_996);
        // Each batch uses file-relative positions and the same selection. The underlying
        // reader may already have skipped rows, for example through Parquet page pruning.
        for (List<Long> positions :
                Arrays.asList(
                        Arrays.asList(99_990L, 99_992L, 99_994L),
                        Arrays.asList(99_996L, 99_998L, 100_000L))) {
            int[] released = {0};
            FileRecordIterator<Long> selected =
                    createPositionIterator(positions, released).selection(selection);
            List<Long> expected = new ArrayList<>();
            for (long position : positions) {
                if (selection.contains((int) position)) {
                    expected.add(position);
                }
            }
            assertThat(collectAll(selected)).isEqualTo(expected);
            selected.releaseBatch();
            assertThat(released[0]).isOne();

            selected = createPositionIterator(positions, released).selection(selection);
            assertThat(selected.skip()).isTrue();
            assertThat(selected.returnedPosition()).isEqualTo(expected.get(0));
            assertThat(collectAll(selected)).isEqualTo(expected.subList(1, expected.size()));
        }
        assertThat(selection.getCardinality()).isEqualTo(99_998);
    }

    @Test
    public void testSelectionAtMaximumPosition() throws IOException {
        RoaringBitmap32 selection =
                RoaringBitmap32.bitmapOf(Integer.MAX_VALUE, Integer.MIN_VALUE, -1);
        FileRecordIterator<Long> selected =
                createPositionIterator(
                                Arrays.asList(
                                        (long) Integer.MAX_VALUE - 1,
                                        (long) Integer.MAX_VALUE,
                                        1L << 31,
                                        0xFFFFFFFFL,
                                        1L << 32),
                                new int[1])
                        .selection(selection);
        assertThat(selected.next()).isEqualTo((long) Integer.MAX_VALUE);
        assertThat(selected.skip()).isTrue();
        assertThat(selected.returnedPosition()).isEqualTo(1L << 31);
        assertThat(selected.next()).isEqualTo(0xFFFFFFFFL);
        assertThat(selected.next()).isNull();
        assertThat(selected.skip()).isFalse();
    }

    private FileRecordIterator<Long> createPositionIterator(List<Long> positions, int[] released) {
        return new FileRecordIterator<Long>() {
            private int index = -1;

            @Override
            public long returnedPosition() {
                return positions.get(index);
            }

            @Override
            public Path filePath() {
                return new Path("test-file.parquet");
            }

            @Override
            public Long next() {
                return skip() ? positions.get(index) : null;
            }

            @Override
            public boolean skip() {
                return ++index < positions.size();
            }

            @Override
            public void releaseBatch() {
                released[0]++;
            }
        };
    }

    @Test
    public void testTransformSkipDoesNotApplyFunction() throws IOException {
        int[] transformCount = {0};
        FileRecordIterator<String> transformed =
                createIterator(Arrays.asList(1L, 2L, 3L))
                        .transform(
                                value -> {
                                    transformCount[0]++;
                                    return value.toString();
                                });

        assertThat(transformed.skip()).isTrue();
        assertThat(transformCount[0]).isZero();
        assertThat(transformed.next()).isEqualTo("2");
        assertThat(transformCount[0]).isOne();
        assertThat(transformed.skip()).isTrue();
        assertThat(transformCount[0]).isOne();
        assertThat(transformed.skip()).isFalse();
    }

    @Test
    public void testSelectionFilePathPreserved() {
        List<Long> values = Arrays.asList(0L, 1L, 2L);
        FileRecordIterator<Long> iterator = createIterator(values);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);

        FileRecordIterator<Long> filteredIterator = iterator.selection(selection);

        assertThat(filteredIterator.filePath().toString()).isEqualTo("test-file.parquet");
    }

    private FileRecordIterator<Long> createIterator(List<Long> values) {
        return new FileRecordIterator<Long>() {
            private int position = -1;

            @Override
            public long returnedPosition() {
                return position;
            }

            @Override
            public Path filePath() {
                return new Path("test-file.parquet");
            }

            @Nullable
            @Override
            public Long next() {
                position++;
                if (position >= values.size()) {
                    return null;
                }
                return values.get(position);
            }

            @Override
            public boolean skip() {
                if (position + 1 >= values.size()) {
                    return false;
                }
                position++;
                return true;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    private List<Long> collectAll(FileRecordIterator<Long> iterator) throws IOException {
        List<Long> result = new ArrayList<>();
        Long value;
        while ((value = iterator.next()) != null) {
            result.add(value);
        }
        return result;
    }
}
