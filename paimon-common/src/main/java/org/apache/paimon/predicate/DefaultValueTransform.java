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

package org.apache.paimon.predicate;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.DefaultValueUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link Transform} which always returns the default value of the input field's {@link DataType}.
 */
public class DefaultValueTransform implements Transform {

    private static final long serialVersionUID = 1L;

    private final FieldRef fieldRef;

    public DefaultValueTransform(FieldRef fieldRef) {
        this.fieldRef = Objects.requireNonNull(fieldRef, "fieldRef must not be null");
    }

    public FieldRef fieldRef() {
        return fieldRef;
    }

    @Override
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    @Override
    public DataType outputType() {
        return fieldRef.type();
    }

    @Override
    public Object transform(InternalRow row) {
        return DefaultValueUtils.defaultValue(fieldRef.type());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        List<Object> nonNullInputs =
                Objects.requireNonNull(inputs, "DefaultValueTransform expects non-null inputs");
        checkArgument(nonNullInputs.size() == 1, "DefaultValueTransform expects 1 input");
        checkArgument(
                nonNullInputs.get(0) instanceof FieldRef,
                "DefaultValueTransform input must be FieldRef");
        return new DefaultValueTransform((FieldRef) nonNullInputs.get(0));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultValueTransform that = (DefaultValueTransform) o;
        return Objects.equals(fieldRef, that.fieldRef);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldRef);
    }

    @Override
    public String toString() {
        return "DefaultValueTransform{" + "fieldRef=" + fieldRef + '}';
    }
}
