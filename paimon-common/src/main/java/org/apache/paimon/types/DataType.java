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
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Describes the data type in the paimon ecosystem.
 *
 * @see DataTypes
 * @since 0.4.0
 */
@Public
public abstract class DataType implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean isNullable;

    private final DataTypeRoot typeRoot;

    public DataType(boolean isNullable, DataTypeRoot typeRoot) {
        this.isNullable = isNullable;
        this.typeRoot = Preconditions.checkNotNull(typeRoot);
    }

    /** Returns whether a value of this type can be {@code null}. */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * Returns the root of this type. It is an essential description without additional parameters.
     */
    public DataTypeRoot getTypeRoot() {
        return typeRoot;
    }

    /**
     * Returns whether the root of the type equals to the {@code typeRoot} or not.
     *
     * @param typeRoot The root type to check against for equality
     */
    public boolean is(DataTypeRoot typeRoot) {
        return this.typeRoot == typeRoot;
    }

    /**
     * Returns whether the root of the type equals to at least on of the {@code typeRoots} or not.
     *
     * @param typeRoots The root types to check against for equality
     */
    public boolean isAnyOf(DataTypeRoot... typeRoots) {
        return Arrays.stream(typeRoots).anyMatch(tr -> this.typeRoot == tr);
    }

    /**
     * Returns whether the root of the type is part of at least one family of the {@code typeFamily}
     * or not.
     *
     * @param typeFamilies The families to check against for equality
     */
    public boolean isAnyOf(DataTypeFamily... typeFamilies) {
        return Arrays.stream(typeFamilies).anyMatch(tf -> this.typeRoot.getFamilies().contains(tf));
    }

    /**
     * Returns whether the family type of the type equals to the {@code family} or not.
     *
     * @param family The family type to check against for equality
     */
    public boolean is(DataTypeFamily family) {
        return typeRoot.getFamilies().contains(family);
    }

    /** The default size of a value of this data type, used internally for size estimation. */
    public abstract int defaultSize();

    /**
     * Returns a deep copy of this type with possibly different nullability.
     *
     * @param isNullable the intended nullability of the copied type
     * @return a deep copy
     */
    public abstract DataType copy(boolean isNullable);

    /**
     * Returns a deep copy of this type. It requires an implementation of {@link #copy(boolean)}.
     *
     * @return a deep copy
     */
    public final DataType copy() {
        return copy(isNullable);
    }

    /**
     * Compare two data types without nullable.
     *
     * @param o the target data type
     */
    public boolean equalsIgnoreNullable(@Nonnull DataType o) {
        return Objects.equals(this.copy(true), o.copy(true));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType that = (DataType) o;
        return isNullable == that.isNullable && typeRoot == that.typeRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isNullable, typeRoot);
    }

    /**
     * Returns a string that summarizes this type as SQL standard string for printing to a console.
     * An implementation might shorten long names or skips very specific properties.
     */
    public abstract String asSQLString();

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeString(asSQLString());
    }

    protected String withNullability(String format, Object... params) {
        if (!isNullable) {
            return String.format(format + " NOT NULL", params);
        }
        return String.format(format, params);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    public abstract <R> R accept(DataTypeVisitor<R> visitor);

    public void collectFieldIds(Set<Integer> fieldIds) {}

    public DataType notNull() {
        return copy(false);
    }

    public DataType nullable() {
        return copy(true);
    }
}
