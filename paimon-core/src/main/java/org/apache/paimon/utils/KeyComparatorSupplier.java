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

package org.apache.paimon.utils;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;

/** A {@link Supplier} that returns the comparator for the file store key. */
public class KeyComparatorSupplier implements SerializableSupplier<Comparator<InternalRow>> {

    private static final long serialVersionUID = 1L;

    private final List<DataType> inputTypes;
    private final int[] sortFields;

    public KeyComparatorSupplier(RowType keyType) {
        this.inputTypes = keyType.getFieldTypes();
        this.sortFields = IntStream.range(0, keyType.getFieldCount()).toArray();
    }

    @Override
    public RecordComparator get() {
        return newRecordComparator(inputTypes, sortFields, true);
    }
}
