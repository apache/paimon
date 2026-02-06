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

package org.apache.paimon.format.lance;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.lance.LanceUtils.toLanceSpecifiedForReader;

/** A factory to create Lance Reader. */
public class LanceReaderFactory implements FormatReaderFactory {

    private final RowType projectedRowType;
    private final int batchSize;

    public LanceReaderFactory(RowType projectedRowType, int batchSize) {
        this.projectedRowType = projectedRowType;
        this.batchSize = batchSize;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        List<Pair<Integer, Integer>> selectionRangesArray = null;
        RoaringBitmap32 roaringBitmap32 = context.selection();
        if (roaringBitmap32 != null) {
            selectionRangesArray = toRangeArray(roaringBitmap32);
        }

        Pair<Path, Map<String, String>> lanceSpecified =
                toLanceSpecifiedForReader(context.fileIO(), context.filePath());
        return new LanceRecordsReader(
                lanceSpecified.getLeft(),
                selectionRangesArray,
                projectedRowType,
                batchSize,
                lanceSpecified.getRight());
    }

    @VisibleForTesting
    List<Pair<Integer, Integer>> toRangeArray(RoaringBitmap32 roaringBitmap32) {
        List<Pair<Integer, Integer>> selectionRangesArray = new ArrayList<>();
        Iterator<Integer> iterator = roaringBitmap32.iterator();
        Integer start = null;
        Integer end = null;
        while (iterator.hasNext()) {
            int value = iterator.next();
            if (start == null) {
                start = value;
                end = start + 1;
                continue;
            }

            if (value == end) {
                end++;
            } else {
                selectionRangesArray.add(Pair.of(start, end));
                start = value;
                end = start + 1;
            }
        }

        if (start != null) {
            selectionRangesArray.add(Pair.of(start, end));
        }
        return selectionRangesArray;
    }
}
