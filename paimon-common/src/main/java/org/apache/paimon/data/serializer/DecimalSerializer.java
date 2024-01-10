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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.math.BigDecimal;

/** Serializer for {@link Decimal}. */
public final class DecimalSerializer implements Serializer<Decimal> {

    private static final long serialVersionUID = 1L;

    private final int precision;
    private final int scale;

    public DecimalSerializer(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public Decimal copy(Decimal from) {
        return from.copy();
    }

    @Override
    public void serialize(Decimal record, DataOutputView target) throws IOException {
        if (Decimal.isCompact(precision)) {
            assert record.isCompact();
            target.writeLong(record.toUnscaledLong());
        } else {
            byte[] bytes = record.toUnscaledBytes();
            BinarySerializer.INSTANCE.serialize(bytes, target);
        }
    }

    @Override
    public Decimal deserialize(DataInputView source) throws IOException {
        if (Decimal.isCompact(precision)) {
            long longVal = source.readLong();
            return Decimal.fromUnscaledLong(longVal, precision, scale);
        } else {
            byte[] bytes = BinarySerializer.INSTANCE.deserialize(source);
            return Decimal.fromUnscaledBytes(bytes, precision, scale);
        }
    }

    @Override
    public String serializeToString(Decimal record) {
        return record.toString();
    }

    @Override
    public Decimal deserializeFromString(String s) {
        return Decimal.fromBigDecimal(new BigDecimal(s), precision, scale);
    }

    @Override
    public DecimalSerializer duplicate() {
        return new DecimalSerializer(precision, scale);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DecimalSerializer that = (DecimalSerializer) o;

        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        int result = precision;
        result = 31 * result + scale;
        return result;
    }
}
