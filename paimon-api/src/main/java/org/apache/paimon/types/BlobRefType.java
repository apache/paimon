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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * Data type of a blob reference.
 *
 * <p>The physical representation is {@code byte[]}. The bytes must conform to the serialization
 * format of {@link org.apache.paimon.data.BlobDescriptor}.
 *
 * @since 1.5.0
 */
@Public
public final class BlobRefType extends DataType {

    private static final long serialVersionUID = 1L;

    // BlobDescriptor is small in most cases (URI + 2 longs).
    public static final int DEFAULT_SIZE = 128;

    private static final String FORMAT = "BLOB_REF";

    public BlobRefType(boolean isNullable) {
        super(isNullable, DataTypeRoot.BLOB_REF);
    }

    public BlobRefType() {
        this(true);
    }

    @Override
    public int defaultSize() {
        return DEFAULT_SIZE;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new BlobRefType(isNullable);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
