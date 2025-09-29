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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Data type of binary large object.
 *
 * @since 1.4.0
 */
@Public
public final class BlobType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_SIZE = 1024 * 1024;

    private static final String FORMAT = "BLOB";

    public BlobType(boolean isNullable) {
        super(isNullable, DataTypeRoot.BLOB);
    }

    public BlobType() {
        this(true);
    }

    @Override
    public int defaultSize() {
        return DEFAULT_SIZE;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new BlobType(isNullable);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static Pair<RowType, RowType> splitBlob(RowType rowType) {
        List<DataField> fields = rowType.getFields();
        List<DataField> normalFields = new ArrayList<>();
        List<DataField> blobFields = new ArrayList<>();

        for (DataField field : fields) {
            DataTypeRoot type = field.type().getTypeRoot();
            if (type == DataTypeRoot.BLOB) {
                blobFields.add(field);
            } else {
                normalFields.add(field);
            }
        }

        return Pair.of(new RowType(normalFields), new RowType(blobFields));
    }
}
