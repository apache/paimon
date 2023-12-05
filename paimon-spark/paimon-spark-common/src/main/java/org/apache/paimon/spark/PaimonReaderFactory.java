/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.spark.SparkUtils.createIOManager;

/** A Spark {@link PartitionReaderFactory} for paimon. */
public class PaimonReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;

    private final ReadBuilder readBuilder;

    public PaimonReaderFactory(ReadBuilder readBuilder) {
        this.readBuilder = readBuilder;
    }

    @Override
    public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(
            InputPartition partition) {
        RecordReader<InternalRow> reader;
        IOManager ioManager = createIOManager();
        try {
            reader =
                    readBuilder
                            .newRead()
                            .withIOManager(ioManager)
                            .createReader(((PaimonInputPartition) partition).split());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader);
        PaimonInternalRow row = new PaimonInternalRow(readBuilder.readType());
        return new PaimonInputPartitionReader(ioManager, iterator, row);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PaimonReaderFactory that = (PaimonReaderFactory) o;
        return this.readBuilder.equals(that.readBuilder);
    }
}
