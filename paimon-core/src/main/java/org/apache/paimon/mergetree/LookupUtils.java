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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BiFunctionWithIOE;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/** Utils for lookup. */
public class LookupUtils {

    public static <T> T lookup(
            Levels levels,
            InternalRow key,
            int startLevel,
            BiFunctionWithIOE<InternalRow, SortedRun, T> lookup,
            BiFunctionWithIOE<InternalRow, TreeSet<DataFileMeta>, T> level0Lookup)
            throws IOException {

        T result = null;
        for (int i = startLevel; i < levels.numberOfLevels(); i++) {
            if (i == 0) {
                result = level0Lookup.apply(key, levels.level0());
            } else {
                SortedRun level = levels.runOfLevel(i);
                result = lookup.apply(key, level);
            }
            if (result != null) {
                break;
            }
        }

        return result;
    }

    public static <T> T lookupLevel0(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            TreeSet<DataFileMeta> level0,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        T result = null;
        for (DataFileMeta file : level0) {
            if (keyComparator.compare(file.maxKey(), target) >= 0
                    && keyComparator.compare(file.minKey(), target) <= 0) {
                result = lookup.apply(target, file);
                if (result != null) {
                    break;
                }
            }
        }

        return result;
    }

    public static <T> T lookup(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            SortedRun level,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        if (level.isEmpty()) {
            return null;
        }
        List<DataFileMeta> files = level.files();
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the
        // targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (keyComparator.compare(files.get(mid).maxKey(), target) < 0) {
                // Key at "mid.max" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.max" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }

        int index = right;

        // if the index is now pointing to the last file, check if the largest key in the block is
        // than the target key.  If so, we need to seek beyond the end of this file
        if (index == files.size() - 1
                && keyComparator.compare(files.get(index).maxKey(), target) < 0) {
            index++;
        }

        // if files does not have a next, it means the key does not exist in this level
        return index < files.size() ? lookup.apply(target, files.get(index)) : null;
    }

    public static int fileKibiBytes(File file) {
        long kibiBytes = file.length() >> 10;
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }
}
