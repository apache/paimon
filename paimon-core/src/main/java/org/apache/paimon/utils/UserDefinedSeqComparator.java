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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link FieldsComparator} for user defined sequence fields. */
public class UserDefinedSeqComparator implements FieldsComparator {

    private final int[] fields;
    private final RecordComparator comparator;

    public UserDefinedSeqComparator(int[] fields, RecordComparator comparator) {
        this.fields = fields;
        this.comparator = comparator;
    }

    @Override
    public int[] compareFields() {
        return fields;
    }

    @Override
    public int compare(InternalRow o1, InternalRow o2) {
        return comparator.compare(o1, o2);
    }

    @Nullable
    public static UserDefinedSeqComparator create(RowType rowType, CoreOptions options) {
        return create(rowType, options.sequenceField());
    }

    @Nullable
    public static UserDefinedSeqComparator create(RowType rowType, List<String> sequenceFields) {
        if (sequenceFields.isEmpty()) {
            return null;
        }

        List<String> fieldNames = rowType.getFieldNames();
        int[] fields = sequenceFields.stream().mapToInt(fieldNames::indexOf).toArray();
        RecordComparator comparator =
                CodeGenUtils.newRecordComparator(rowType.getFieldTypes(), fields);
        return new UserDefinedSeqComparator(fields, comparator);
    }
}
