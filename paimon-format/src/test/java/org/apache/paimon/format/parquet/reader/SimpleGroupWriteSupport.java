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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.format.parquet.writer.StreamOutputFile;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** For writing two level representations for parquet list type. */
class SimpleGroupWriteSupport extends WriteSupport<SimpleGroupWriteSupport.SimpleGroup> {

    private org.apache.parquet.schema.MessageType schema;
    private RecordConsumer recordConsumer;

    static class SimpleGroup {
        List<Integer> numbers;

        public SimpleGroup(List<Integer> numbers) {
            this.numbers = numbers;
        }
    }

    @Override
    public WriteContext init(Configuration configuration) {
        String schemaString =
                "message Record { required group list_of_ints (LIST) { repeated int32 list_of_ints_tuple; } }";
        this.schema = MessageTypeParser.parseMessageType(schemaString);
        return new WriteContext(schema, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(SimpleGroup record) {
        recordConsumer.startMessage();

        int listGroupIndex = schema.getFieldIndex("list_of_ints");
        String listGroupName = schema.getFieldName(listGroupIndex);
        GroupType listGroupType = schema.getType(listGroupIndex).asGroupType();

        int elementsFieldIndex = listGroupType.getFieldIndex("list_of_ints_tuple");
        String elementsFieldName = listGroupType.getFieldName(elementsFieldIndex);
        recordConsumer.startField(listGroupName, listGroupIndex);

        if (record != null && record.numbers != null) {
            recordConsumer.startField(elementsFieldName, elementsFieldIndex);
            for (Integer number : record.numbers) {
                recordConsumer.addInteger(number);
            }
            recordConsumer.endField(elementsFieldName, elementsFieldIndex);
        }
        recordConsumer.endField(listGroupName, listGroupIndex);
        recordConsumer.endMessage();
    }

    static class Builder
            extends ParquetWriter.Builder<SimpleGroupWriteSupport.SimpleGroup, Builder> {

        protected Builder(OutputFile path) {
            super(path);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<SimpleGroupWriteSupport.SimpleGroup> getWriteSupport(
                Configuration conf) {
            return new SimpleGroupWriteSupport();
        }
    }

    public void writeTest(String path, List<SimpleGroupWriteSupport.SimpleGroup> records)
            throws IOException {
        Configuration conf = new Configuration();

        ParquetWriter<SimpleGroupWriteSupport.SimpleGroup> writer = null;
        try {

            writer =
                    new Builder(
                                    new StreamOutputFile(
                                            new LocalFileIO.LocalPositionOutputStream(
                                                    new File(path))))
                            .withConf(conf)
                            .build();
            for (SimpleGroupWriteSupport.SimpleGroup record : records) {
                writer.write(record);
            }

        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
