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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.FieldReaderFactory.RowReader;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;

/** A {@link DatumReader} for reading {@link InternalRow}. */
public class AvroRowDatumReader implements DatumReader<InternalRow> {

    private final RowType projectedRowType;

    private RowReader reader;
    private boolean isUnion;

    public AvroRowDatumReader(RowType projectedRowType) {
        this.projectedRowType = projectedRowType;
    }

    @Override
    public void setSchema(Schema schema) {
        this.isUnion = false;
        if (schema.isUnion()) {
            this.isUnion = true;
            schema = schema.getTypes().get(1);
        }
        this.reader =
                new FieldReaderFactory().createRowReader(schema, projectedRowType.getFields());
    }

    @Override
    public InternalRow read(InternalRow reuse, Decoder in) throws IOException {
        if (isUnion) {
            int index = in.readIndex();
            if (index == 0) {
                throw new RuntimeException("Cannot read a null row.");
            }
        }

        return reader.read(in, reuse);
    }
}
