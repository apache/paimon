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

package org.apache.paimon.format.orc.writer;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * This class provides an abstracted set of methods to handle the lifecycle of {@link
 * VectorizedRowBatch}.
 *
 * <p>Users have to extend this class and override the vectorize() method with the logic to
 * transform the element to a {@link VectorizedRowBatch}.
 *
 * @param <T> The type of the element
 */
public abstract class Vectorizer<T> implements Serializable {

    private final TypeDescription schema;

    public Vectorizer(final String schema) {
        checkNotNull(schema);
        this.schema = TypeDescription.fromString(schema);
    }

    /**
     * Provides the ORC schema.
     *
     * @return the ORC schema
     */
    public TypeDescription getSchema() {
        return this.schema;
    }

    /**
     * Transforms the provided element to ColumnVectors and sets them in the exposed
     * VectorizedRowBatch.
     *
     * @param element The input element
     * @param batch The batch to write the ColumnVectors
     * @throws IOException if there is an error while transforming the input.
     */
    public abstract void vectorize(T element, VectorizedRowBatch batch) throws IOException;
}
