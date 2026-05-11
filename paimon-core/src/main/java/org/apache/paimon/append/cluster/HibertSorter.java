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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.hilbert.HilbertIndexer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Hilbert sorter for clustering. */
public class HibertSorter extends Sorter {

    private static final RowType KEY_TYPE =
            new RowType(Collections.singletonList(new DataField(0, "H_INDEX", DataTypes.BYTES())));

    private final HibertKeyAbstract hilbertKeyAbstract;

    public HibertSorter(
            RecordReaderIterator<InternalRow> reader,
            RowType valueType,
            CoreOptions options,
            List<String> orderColNames,
            IOManager ioManager) {
        super(reader, KEY_TYPE, valueType, options, ioManager);
        this.hilbertKeyAbstract = new HibertKeyAbstract(valueType, orderColNames);
        this.hilbertKeyAbstract.open();
    }

    @Override
    public InternalRow assignSortKey(InternalRow row) {
        byte[] key = hilbertKeyAbstract.apply(row);
        return new JoinedRow(GenericRow.of(key), row);
    }

    private static class HibertKeyAbstract implements KeyAbstract<byte[]> {

        private final HilbertIndexer hilbertIndexer;

        public HibertKeyAbstract(RowType rowType, List<String> orderColNames) {
            hilbertIndexer = new HilbertIndexer(rowType, orderColNames);
        }

        @Override
        public void open() {
            hilbertIndexer.open();
        }

        @Override
        public byte[] apply(InternalRow value) {
            byte[] hilbert = hilbertIndexer.index(value);
            return Arrays.copyOf(hilbert, hilbert.length);
        }
    }
}
