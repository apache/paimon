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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.get;

/** Transform that casts a field to a new type. */
public class CastTransform implements Transform {

    private static final long serialVersionUID = 1L;

    private final FieldRef fieldRef;
    private final DataType type;
    private transient CastExecutor<Object, Object> cast;

    private CastTransform(FieldRef fieldRef, DataType type, CastExecutor<Object, Object> cast) {
        this.fieldRef = fieldRef;
        this.type = type;
        this.cast = cast;
    }

    public static Optional<Transform> tryCreate(FieldRef fieldRef, DataType type) {
        if (fieldRef.type().equals(type)) {
            return Optional.of(new FieldTransform(fieldRef));
        }

        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> cast =
                (CastExecutor<Object, Object>) CastExecutors.resolve(fieldRef.type(), type);
        if (cast == null) {
            return Optional.empty();
        } else {
            return Optional.of(new CastTransform(fieldRef, type, cast));
        }
    }

    @Override
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    @Override
    public DataType outputType() {
        return type;
    }

    @Override
    public Object transform(InternalRow row) {
        return cast.cast(get(row, fieldRef.index(), fieldRef.type()));
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        assert inputs.size() == 1;
        return new CastTransform((FieldRef) inputs.get(0), type, cast);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CastTransform that = (CastTransform) o;
        return Objects.equals(fieldRef, that.fieldRef) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldRef, type);
    }

    @Override
    public String toString() {
        return "CAST( " + fieldRef + " AS " + type + ")";
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolved =
                (CastExecutor<Object, Object>) CastExecutors.resolve(fieldRef.type(), type);
        this.cast = resolved;
    }
}
