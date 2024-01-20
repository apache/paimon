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

package org.apache.paimon.stats;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Stats}, providing utility methods related to paths and stats hints. */
public class StatisticsManager {
    private final FileIO fileIO;
    private static final String STATISTIC_PREFIX = "stats-";
    private final Path tablePath;

    public StatisticsManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public Iterator<Stats> statistics() throws IOException {
        return listVersionedFiles(fileIO, statisticsDirectory(), STATISTIC_PREFIX)
                .map(this::statistic)
                .sorted(Comparator.comparingLong(Stats::snapshotId))
                .iterator();
    }

    public Stats statistic(long statisticsId) {
        return Stats.fromPath(fileIO, statisticPath(statisticsId));
    }

    public Path statisticPath(long statisticsId) {
        return new Path(tablePath + "/statistics/" + STATISTIC_PREFIX + statisticsId);
    }

    public Path statisticsDirectory() {
        return new Path(tablePath + "/statistics");
    }

    public long statisticsCount() throws IOException {
        return listVersionedFiles(fileIO, statisticsDirectory(), STATISTIC_PREFIX).count();
    }
}
