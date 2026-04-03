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

import dev.vortex.api.Array;
import dev.vortex.api.ArrayIterator;
import dev.vortex.api.DType;

import java.util.Optional;
import java.util.OptionalLong;

/** JNI implementation of the ArrayIterator interface. */
public final class JNIArrayIterator implements ArrayIterator {
    private OptionalLong pointer;
    private Optional<Array> next;

    public JNIArrayIterator(long pointer) {
        Preconditions.checkArgument(pointer > 0, "Invalid pointer address: " + pointer);
        this.pointer = OptionalLong.of(pointer);
        advance();
    }

    @Override
    public boolean hasNext() {
        return next.isPresent();
    }

    @Override
    public Array next() {
        Array array = this.next.get();
        advance();
        return array;
    }

    @Override
    public DType getDataType() {
        return new JNIDType(NativeArrayIteratorMethods.getDType(pointer.getAsLong()));
    }

    @Override
    public void close() {
        if (!pointer.isPresent()) {
            return;
        }
        NativeArrayIteratorMethods.free(pointer.getAsLong());
        pointer = OptionalLong.empty();
        next = Optional.empty();
    }

    private void advance() {
        long next = NativeArrayIteratorMethods.take(pointer.getAsLong());
        if (next <= 0) {
            this.next = Optional.empty();
        } else {
            this.next = Optional.of(new JNIArray(next));
        }
    }
}
