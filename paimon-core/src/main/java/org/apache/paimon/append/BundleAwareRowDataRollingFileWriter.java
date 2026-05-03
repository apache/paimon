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

package org.apache.paimon.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import java.util.function.Supplier;

/** Rolling writer used by dedicated-format bundle pass-through paths. */
class BundleAwareRowDataRollingFileWriter extends RollingFileWriterImpl<InternalRow, DataFileMeta>
        implements BundlePassThroughWriter {

    private final boolean supportsBundlePassThrough;

    public BundleAwareRowDataRollingFileWriter(
            Supplier<? extends BundleAwareRowDataFileWriter> writerFactory,
            boolean supportsBundlePassThrough,
            long targetFileSize) {
        super(writerFactory, targetFileSize);
        this.supportsBundlePassThrough = supportsBundlePassThrough;
    }

    @Override
    public boolean supportsBundlePassThrough() {
        return supportsBundlePassThrough;
    }

    @Override
    public void writeReplayableBundle(ReplayableBundleRecords bundle) throws java.io.IOException {
        writeBundle(bundle);
    }

    @VisibleForTesting
    public static boolean supportsBundlePassThrough(
            FileFormat fileFormat,
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors) {
        return !RollingFileWriter.createStatsProducer(fileFormat, rowType, statsCollectors)
                .requirePerRecord();
    }
}
