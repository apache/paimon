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
import org.apache.paimon.format.avro.FieldWriterFactory.RowWriter;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;

/** A {@link DatumWriter} for writing {@link InternalRow}. */
public class AvroRowDatumWriter implements DatumWriter<InternalRow> {

    private final RowType rowType;

    private RowWriter writer;
    private boolean isUnion;

    public AvroRowDatumWriter(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void setSchema(Schema schema) {
        this.isUnion = false;
        if (schema.isUnion()) {
            this.isUnion = true;
            schema = schema.getTypes().get(1);
        }
        this.writer = new FieldWriterFactory().createRowWriter(schema, rowType.getFields());
    }

    @Override
    public void write(InternalRow datum, Encoder out) throws IOException {
        if (isUnion) {
            // top Row is a UNION type
            out.writeIndex(1);
        }
        try {
            this.writer.writeRow(datum, out);
        } catch (NullPointerException npe) {
            throw new RuntimeException(
                    "Caught NullPointerException, the possible reason is you have set following options together:\n"
                            + "  1. file.format = avro;\n"
                            + "  2. merge-function = aggregation/partial-update;\n"
                            + "  3. some fields are not null.",
                    npe);
        }
    }
}
