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

package dev.vortex.api.expressions;

import dev.vortex.api.Expression;
import java.util.List;
import java.util.Optional;

/** Generic expression deserialized from Vortex without a concrete Java type. */
public final class Unknown implements Expression {
    private final String id;
    private final List<Expression> children;
    private final byte[] metadata;

    public Unknown(String id, List<Expression> children, byte[] metadata) {
        this.id = id;
        this.children = children;
        this.metadata = metadata;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public List<Expression> children() {
        return children;
    }

    @Override
    public Optional<byte[]> metadata() {
        return Optional.of(metadata);
    }
}
