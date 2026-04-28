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
import dev.vortex.api.ArrayIterator;
import dev.vortex.api.DType;
import dev.vortex.api.File;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.proto.Expressions;
import java.util.OptionalLong;

public final class JNIFile implements File {
    private OptionalLong pointer;

    public JNIFile(long pointer) {
        Preconditions.checkArgument(pointer > 0, "Invalid pointer address: " + pointer);
        this.pointer = OptionalLong.of(pointer);
    }

    @Override
    public DType getDType() {
        return new JNIDType(NativeFileMethods.dtype(pointer.getAsLong()));
    }

    @Override
    public long rowCount() {
        return NativeFileMethods.rowCount(pointer.getAsLong());
    }

    @Override
    public ArrayIterator newScan(ScanOptions options) {
        byte[] predicateProto = null;

        if (options.predicate().isPresent()) {
            predicateProto = Expressions.serialize(options.predicate().get()).toByteArray();
        }

        long[] rowIndices = options.rowIndices().orElse(null);
        long[] rowRange = options.rowRange().orElse(null);

        return new JNIArrayIterator(
                NativeFileMethods.scan(pointer.getAsLong(), options.columns(), predicateProto, rowRange, rowIndices));
    }

    @Override
    public void close() {
        if (!pointer.isPresent()) {
            return;
        }
        NativeFileMethods.close(pointer.getAsLong());
        pointer = OptionalLong.empty();
    }
}
