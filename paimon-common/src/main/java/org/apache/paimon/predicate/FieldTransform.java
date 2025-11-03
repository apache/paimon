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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.InternalRowUtils.get;

/** Transform that extracts a field from a row. */
public class FieldTransform implements Transform {

    private static final long serialVersionUID = 1L;

    private final FieldRef fieldRef;

    public FieldTransform(FieldRef fieldRef) {
        this.fieldRef = fieldRef;
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
        return get(row, fieldRef.index(), fieldRef.type());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        assert inputs.size() == 1;
        return new FieldTransform((FieldRef) inputs.get(0));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldTransform that = (FieldTransform) o;
        return Objects.equals(fieldRef, that.fieldRef);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldRef);
    }
}
