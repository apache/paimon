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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StatsFile}. */
public class StatsFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void test() throws IOException {
        Path dir = new Path(tempPath.toUri());
        PathFactory pathFactory =
                new PathFactory() {
                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public Path toPath(String fileName) {
                        return new Path(dir, fileName);
                    }
                };

        StatsFile file = new StatsFile(LocalFileIO.create(), pathFactory);
        HashMap<String, ColStats<?>> colStatsMap = new HashMap<>();
        colStatsMap.put("orderId", new ColStats<>(0, 10L, "111", "222", 0L, 8L, 8L));
        Statistics stats = new Statistics(1L, 0L, 10L, 1000L, colStatsMap);
        String fileName = file.write(stats);

        assertThat(file.exists(fileName)).isTrue();

        assertThat(file.read(fileName)).isEqualTo(stats);

        file.delete(fileName);

        assertThat(file.exists(fileName)).isFalse();
    }
}
