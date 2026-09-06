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

import java.util.Locale;
import java.util.Objects;

import static org.apache.paimon.utils.EncodingUtils.escapeSingleQuotes;

/** Planar geometry encoded as OGC Well-Known Binary. */
@Public
public class GeometryType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CRS = "OGC:CRS84";

    private static final String FORMAT = "GEOMETRY(%s)";

    private final String crs;

    public GeometryType(boolean isNullable, String crs) {
        super(isNullable, DataTypeRoot.GEOMETRY);
        this.crs = validateCrs(crs == null ? DEFAULT_CRS : crs);
    }

    public GeometryType(String crs) {
        this(true, crs);
    }

    public GeometryType() {
        this(DEFAULT_CRS);
    }

    public String getCrs() {
        return crs;
    }

    @Override
    public int defaultSize() {
        return 20;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new GeometryType(isNullable, crs);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, formatCrs(crs));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }
        GeometryType that = (GeometryType) o;
        return crs.equalsIgnoreCase(that.crs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), crs.toUpperCase(Locale.ROOT));
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    static String validateCrs(String crs) {
        if (crs.isEmpty()) {
            throw new IllegalArgumentException("Invalid CRS: " + crs);
        }
        return crs;
    }

    static String formatCrs(String crs) {
        if (Character.isDigit(crs.charAt(0))) {
            return "'" + escapeSingleQuotes(crs) + "'";
        }
        for (int i = 0; i < crs.length(); i++) {
            char character = crs.charAt(i);
            if (Character.isWhitespace(character)
                    || character == '<'
                    || character == '>'
                    || character == '('
                    || character == ')'
                    || character == ','
                    || character == '.'
                    || character == '\''
                    || character == '`') {
                return "'" + escapeSingleQuotes(crs) + "'";
            }
        }
        return crs;
    }
}
