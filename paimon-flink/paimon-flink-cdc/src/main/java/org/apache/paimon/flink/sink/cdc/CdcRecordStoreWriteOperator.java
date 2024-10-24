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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord}. Record schema may change. If current
 * known schema does not fit record schema, this operator will wait for schema changes.
 */
public class CdcRecordStoreWriteOperator extends TableWriteOperator<CdcRecord> {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<Duration> RETRY_SLEEP_TIME =
            ConfigOptions.key("cdc.retry-sleep-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500));

    public static final ConfigOption<Integer> MAX_RETRY_NUM_TIMES =
            ConfigOptions.key("cdc.max-retry-num-times")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Max retry count for updating table before failing loudly");

    public static final ConfigOption<Boolean> SKIP_CORRUPT_RECORD =
            ConfigOptions.key("cdc.skip-corrupt-record")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip corrupt record if we fail to parse it");

    private final long retrySleepMillis;

    private final int maxRetryNumTimes;

    private final boolean skipCorruptRecord;

    public CdcRecordStoreWriteOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
        this.retrySleepMillis =
                table.coreOptions().toConfiguration().get(RETRY_SLEEP_TIME).toMillis();
        this.maxRetryNumTimes = table.coreOptions().toConfiguration().get(MAX_RETRY_NUM_TIMES);
        this.skipCorruptRecord = table.coreOptions().toConfiguration().get(SKIP_CORRUPT_RECORD);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        table = table.copyWithLatestSchema();
        super.initializeState(context);
    }

    @Override
    protected boolean containLogSystem() {
        return false;
    }

    @Override
    public void processElement(StreamRecord<CdcRecord> element) throws Exception {
        CdcRecord record = element.getValue();
        Optional<GenericRow> optionalConverted = toGenericRow(record, table.schema().fields());
        if (!optionalConverted.isPresent()) {
            for (int retry = 0; retry < maxRetryNumTimes; ++retry) {
                table = table.copyWithLatestSchema();
                optionalConverted = toGenericRow(record, table.schema().fields());
                if (optionalConverted.isPresent()) {
                    break;
                }
                Thread.sleep(retrySleepMillis);
            }
            write.replace(table);
        }

        if (!optionalConverted.isPresent()) {
            if (skipCorruptRecord) {
                LOG.warn("Skipping corrupt or unparsable record {}", record);
            } else {
                throw new RuntimeException("Unable to process element. Possibly a corrupt record");
            }
        } else {
            try {
                write.write(optionalConverted.get());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
