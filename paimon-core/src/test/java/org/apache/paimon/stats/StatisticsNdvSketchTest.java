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
import org.apache.paimon.utils.ThetaSketch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Test for {@link StatisticsNdvSketch}. */
public class StatisticsNdvSketchTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testMergeableNdvSketchBlobRoundTrip() {
        byte[] left = ThetaSketch.sketchOf(1, 2, 3);
        byte[] right = ThetaSketch.sketchOf(3, 4, 5);
        byte[] union = StatisticsNdvSketch.union(left, right);

        assertThat(StatisticsNdvSketch.estimate(union)).isCloseTo(5.0, offset(0.001));

        StatisticsBlob blob = StatisticsNdvSketch.toBlob(1, 10L, 20L, union);
        StatisticsSidecarFile sidecarFile =
                new StatisticsSidecarFile(LocalFileIO.create(), pathFactory());
        List<StatisticsBlobMetadata> metadata = sidecarFile.write(Collections.singletonList(blob));

        StatisticsBlobMetadata ndvMetadata = metadata.get(0);
        assertThat(ndvMetadata.type()).isEqualTo(StatisticsNdvSketch.THETA_SKETCH_BLOB_TYPE);
        assertThat(ndvMetadata.fieldIds()).containsExactly(1);
        assertThat(StatisticsNdvSketch.ndv(ndvMetadata)).isCloseTo(5.0, offset(0.001));

        byte[] payload = sidecarFile.read(ndvMetadata);
        assertThat(StatisticsNdvSketch.estimate(payload)).isCloseTo(5.0, offset(0.001));
    }

    private PathFactory pathFactory() {
        Path dir = new Path(tempPath.toUri());
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(dir, UUID.randomUUID().toString());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(dir, fileName);
            }
        };
    }
}
