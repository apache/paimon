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
