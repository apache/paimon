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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** CompactionBase. */
public class CompactionTestBase {

    protected List<LevelSortedRun> level0(long... sizes) {
        List<LevelSortedRun> runs = new ArrayList<>();
        for (Long size : sizes) {
            runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file(size))));
        }
        return runs;
    }

    protected LevelSortedRun level(int level, long size) {
        return new LevelSortedRun(level, SortedRun.fromSingle(file(size)));
    }

    protected List<LevelSortedRun> level(int level, String input) {
        List<DataFileMeta> dataFileMetas = parseMetas(input);
        List<LevelSortedRun> runs = new ArrayList<>();
        runs.add(new LevelSortedRun(level, SortedRun.fromSorted(dataFileMetas)));
        return runs;
    }

    static DataFileMeta file(long size) {
        return new DataFileMeta("", size, 1, null, null, null, null, 0, 0, 0, 0);
    }

    private List<DataFileMeta> parseMetas(String input) {
        List<DataFileMeta> metas = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\[(\\d+?), (\\d+?), (\\d+?)]");
        Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            metas.add(
                    makeInterval(
                            Long.parseLong(matcher.group(1)),
                            Integer.parseInt(matcher.group(2)),
                            Integer.parseInt(matcher.group(3))));
        }
        return metas;
    }

    static DataFileMeta makeInterval(long size, int left, int right) {
        BinaryRow minKey = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minKey);
        minWriter.writeInt(0, left);
        minWriter.complete();
        BinaryRow maxKey = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxKey);
        maxWriter.writeInt(0, right);
        maxWriter.complete();
        return new DataFileMeta("", size, 1, minKey, maxKey, null, null, 0, 0, 0, 0);
    }
}
