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

package org.apache.paimon.index;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Metadata of deletion vector. */
public class DeletionVectorMeta {

    public static final RowType SCHEMA =
            RowType.of(
                    new DataField(0, "f0", newStringType(false)),
                    new DataField(1, "f1", new IntType(false)),
                    new DataField(2, "f2", new IntType(false)),
                    new DataField(3, "_CARDINALITY", new BigIntType(true)));

    private final String dataFileName;
    private final int offset;
    private final int length;
    @Nullable private final Long cardinality;

    public DeletionVectorMeta(
            String dataFileName, int start, int size, @Nullable Long cardinality) {
        this.dataFileName = dataFileName;
        this.offset = start;
        this.length = size;
        this.cardinality = cardinality;
    }

    public String dataFileName() {
        return dataFileName;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    @Nullable
    public Long cardinality() {
        return cardinality;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletionVectorMeta that = (DeletionVectorMeta) o;
        return offset == that.offset
                && length == that.length
                && Objects.equals(dataFileName, that.dataFileName)
                && Objects.equals(cardinality, that.cardinality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFileName, offset, length, cardinality);
    }

    @Override
    public String toString() {
        return "DeletionVectorMeta{"
                + "dataFileName='"
                + dataFileName
                + '\''
                + ", offset="
                + offset
                + ", length="
                + length
                + ", cardinality="
                + cardinality
                + '}';
    }
}
