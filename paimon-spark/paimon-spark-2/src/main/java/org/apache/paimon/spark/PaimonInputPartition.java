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

package org.apache.paimon.spark;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.io.UncheckedIOException;

/** A Spark {@link InputPartition} for paimon. */
public class PaimonInputPartition
        implements InputPartition<org.apache.spark.sql.catalyst.InternalRow> {

    private static final long serialVersionUID = 1L;

    private final ReadBuilder readBuilder;
    private final Split split;

    public PaimonInputPartition(ReadBuilder readBuilder, Split split) {
        this.readBuilder = readBuilder;
        this.split = split;
    }

    @Override
    public InputPartitionReader<org.apache.spark.sql.catalyst.InternalRow> createPartitionReader() {
        RecordReader<InternalRow> recordReader;
        try {
            recordReader = readBuilder.newRead().createReader(split);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        PaimonInternalRow row = new PaimonInternalRow(readBuilder.readType());
        return new PaimonInputPartitionReader(iterator, row);
    }

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}
