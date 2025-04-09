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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;

/** Produce {@link SimpleColStats} for fields. */
public interface SimpleStatsProducer {

    boolean isStatsDisabled();

    boolean requirePerRecord();

    void collect(InternalRow row);

    SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException;

    static SimpleStatsProducer disabledProducer() {
        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return true;
            }

            @Override
            public boolean requirePerRecord() {
                return false;
            }

            @Override
            public void collect(InternalRow row) {
                throw new IllegalStateException();
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
                throw new IllegalStateException();
            }
        };
    }

    static SimpleStatsProducer fromExtractor(@Nullable SimpleStatsExtractor extractor) {
        if (extractor == null) {
            return disabledProducer();
        }

        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return false;
            }

            @Override
            public boolean requirePerRecord() {
                return false;
            }

            @Override
            public void collect(InternalRow row) {}

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length)
                    throws IOException {
                return extractor.extract(fileIO, path, length);
            }
        };
    }

    static SimpleStatsProducer fromCollector(SimpleStatsCollector collector) {
        if (collector.isDisabled()) {
            return disabledProducer();
        }

        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return collector.isDisabled();
            }

            @Override
            public boolean requirePerRecord() {
                return true;
            }

            @Override
            public void collect(InternalRow row) {
                collector.collect(row);
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
                return collector.extract();
            }
        };
    }
}
