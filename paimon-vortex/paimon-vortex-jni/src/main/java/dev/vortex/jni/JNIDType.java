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

package dev.vortex.jni;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import dev.vortex.api.DType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** JNI implementation of the DType interface. */
public final class JNIDType implements DType {
    OptionalLong pointer;
    final boolean isOwned; // True if this object owns the native memory

    public JNIDType(long pointer) {
        this(pointer, false);
    }

    public long getPointer() {
        return pointer.getAsLong();
    }

    public JNIDType(long pointer, boolean isOwned) {
        Preconditions.checkArgument(pointer > 0, "Invalid pointer address: " + pointer);
        this.pointer = OptionalLong.of(pointer);
        this.isOwned = isOwned;
    }

    @Override
    public Variant getVariant() {
        return Variant.from(NativeDTypeMethods.getVariant(pointer.getAsLong()));
    }

    @Override
    public boolean isNullable() {
        return NativeDTypeMethods.isNullable(pointer.getAsLong());
    }

    @Override
    public List<String> getFieldNames() {
        return NativeDTypeMethods.getFieldNames(pointer.getAsLong());
    }

    @Override
    public List<DType> getFieldTypes() {
        return Lists.transform(NativeDTypeMethods.getFieldTypes(pointer.getAsLong()), JNIDType::new);
    }

    @Override
    public DType getElementType() {
        // Returns a borrowed reference - the parent DType owns this memory
        return new JNIDType(NativeDTypeMethods.getElementType(pointer.getAsLong()));
    }

    @Override
    public int getFixedSizeListSize() {
        return NativeDTypeMethods.getFixedSizeListSize(pointer.getAsLong());
    }

    @Override
    public boolean isDate() {
        return NativeDTypeMethods.isDate(pointer.getAsLong());
    }

    @Override
    public boolean isTime() {
        return NativeDTypeMethods.isTime(pointer.getAsLong());
    }

    @Override
    public boolean isTimestamp() {
        return NativeDTypeMethods.isTimestamp(pointer.getAsLong());
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.from(NativeDTypeMethods.getTimeUnit(pointer.getAsLong()));
    }

    @Override
    public Optional<String> getTimeZone() {
        return Optional.ofNullable(NativeDTypeMethods.getTimeZone(pointer.getAsLong()));
    }

    @Override
    public boolean isDecimal() {
        return NativeDTypeMethods.isDecimal(pointer.getAsLong());
    }

    @Override
    public int getPrecision() {
        return NativeDTypeMethods.getDecimalPrecision(pointer.getAsLong());
    }

    @Override
    public byte getScale() {
        return NativeDTypeMethods.getDecimalScale(pointer.getAsLong());
    }

    @Override
    public void close() {
        if (isOwned && pointer.isPresent()) {
            NativeDTypeMethods.free(pointer.getAsLong());
            pointer = OptionalLong.empty();
        }
    }

    public static JNIDType ownedDType(long pointer) {
        return new JNIDType(pointer, true);
    }
}
