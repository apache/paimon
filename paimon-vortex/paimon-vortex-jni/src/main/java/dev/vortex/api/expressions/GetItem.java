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

import com.google.protobuf.InvalidProtocolBufferException;
import dev.vortex.api.Expression;
import dev.vortex.proto.ExprProtos;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a "get item" expression that extracts a field or property from a parent expression.
 * This expression is used to access nested fields in complex data structures such as structs,
 * lists, or other composite types by specifying a path to the desired field.
 */
public final class GetItem implements Expression {
    private final String path;
    private final Expression child;

    private GetItem(Expression child, String path) {
        this.child = child;
        this.path = path;
    }

    /**
     * Creates a new GetItem expression that extracts the specified field from the given child expression.
     *
     * @param child the parent expression from which to extract the field
     * @param path the path or name of the field to extract
     * @return a new GetItem expression
     */
    public static GetItem of(Expression child, String path) {
        return new GetItem(child, path);
    }

    /**
     * Parses a GetItem expression from serialized metadata and child expressions.
     * This method is used during deserialization of Vortex expressions.
     *
     * @param metadata the serialized metadata containing the field path information
     * @param children the child expressions, must contain exactly one element
     * @return a new GetItem expression parsed from the provided data
     * @throws RuntimeException if the number of children is not exactly one,
     *                                  or if the metadata cannot be parsed
     */
    public static GetItem parse(byte[] metadata, List<Expression> children) {
        if (children.size() != 1) {
            throw new IllegalArgumentException(
                    "GetItem expression must have exactly one child, found: " + children.size());
        }
        try {
            ExprProtos.GetItemOpts opts = ExprProtos.GetItemOpts.parseFrom(metadata);
            return new GetItem(children.get(0), opts.getPath());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to parse GetItem metadata", e);
        }
    }

    /**
     * Returns the child expression from which the field is being extracted.
     *
     * @return the child expression
     */
    public Expression getChild() {
        return child;
    }

    /**
     * Returns the path or name of the field being extracted from the child expression.
     *
     * @return the field path
     */
    public String getPath() {
        return path;
    }

    @Override
    public String id() {
        return "vortex.get_item";
    }

    @Override
    public List<Expression> children() {
        return java.util.Collections.singletonList(child);
    }

    @Override
    public Optional<byte[]> metadata() {
        return Optional.of(
                ExprProtos.GetItemOpts.newBuilder().setPath(path).build().toByteArray());
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visitGetItem(this);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GetItem)) return false;
        GetItem getItem = (GetItem) o;
        return Objects.equals(path, getItem.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(path);
    }
}
