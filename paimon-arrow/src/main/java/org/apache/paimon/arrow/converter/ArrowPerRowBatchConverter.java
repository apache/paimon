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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.io.UncheckedIOException;

/** To convert iterator row by row. */
public class ArrowPerRowBatchConverter extends ArrowBatchConverter {

    private InternalRow currentRow;

    public ArrowPerRowBatchConverter(VectorSchemaRoot root, ArrowFieldWriter[] fieldWriters) {
        super(root, fieldWriters);
    }

    @Override
    public void doWrite(int maxBatchRows) {
        int rowIndex = 0;
        while (currentRow != null && rowIndex < maxBatchRows) {
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i].write(rowIndex, currentRow, i);
            }
            nextRow();
            rowIndex++;
        }
        root.setRowCount(rowIndex);

        if (currentRow == null) {
            releaseIterator();
        }
    }

    public void reset(RecordReader.RecordIterator<InternalRow> iterator) {
        this.iterator = iterator;
        nextRow();
    }

    private void nextRow() {
        try {
            currentRow = iterator.next();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
