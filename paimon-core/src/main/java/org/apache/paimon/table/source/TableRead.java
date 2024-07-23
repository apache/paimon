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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstraction layer above {@link SplitRead} to provide reading of {@link InternalRow}.
 *
 * @since 0.4.0
 */
@Public
public interface TableRead {

    TableRead executeFilter();

    TableRead withIOManager(IOManager ioManager);

    RecordReader<InternalRow> createReader(Split split) throws IOException;

    default RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> createReader(split));
        }
        return ConcatRecordReader.create(readers);
    }

    default RecordReader<InternalRow> createReader(TableScan.Plan plan) throws IOException {
        return createReader(plan.splits());
    }
}
