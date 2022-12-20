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

package org.apache.flink.table.store.file.casting;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;

import javax.annotation.Nullable;

/** Cast byte to decimal. */
public class ByteToDecimalCastExecutor implements CastExecutor<Byte, DecimalData> {
    private final int precision;
    private final int scale;

    public ByteToDecimalCastExecutor(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Nullable
    @Override
    public DecimalData cast(@Nullable Byte value) throws TableException {
        return value == null ? null : DecimalDataUtils.castFrom((long) value, precision, scale);
    }
}
