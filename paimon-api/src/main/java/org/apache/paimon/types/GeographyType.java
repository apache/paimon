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

/** Geography encoded as OGC Well-Known Binary. */
@Public
public class GeographyType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CRS = "OGC:CRS84";

    public static final EdgeAlgorithm DEFAULT_ALGORITHM = EdgeAlgorithm.SPHERICAL;

    private static final String FORMAT = "GEOGRAPHY(%s, %s)";

    private final String crs;

    private final EdgeAlgorithm algorithm;

    public GeographyType(boolean isNullable, String crs, EdgeAlgorithm algorithm) {
        super(isNullable, DataTypeRoot.GEOGRAPHY);
        this.crs = GeometryType.validateCrs(crs == null ? DEFAULT_CRS : crs);
        this.algorithm = algorithm == null ? DEFAULT_ALGORITHM : algorithm;
    }

    public GeographyType(String crs, EdgeAlgorithm algorithm) {
        this(true, crs, algorithm);
    }

    public GeographyType(String crs) {
        this(crs, DEFAULT_ALGORITHM);
    }

    public GeographyType() {
        this(DEFAULT_CRS);
    }

    public String getCrs() {
        return crs;
    }

    public EdgeAlgorithm getAlgorithm() {
        return algorithm;
    }

    @Override
    public int defaultSize() {
        return 20;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new GeographyType(isNullable, crs, algorithm);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, GeometryType.formatCrs(crs), algorithm);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }
        GeographyType that = (GeographyType) o;
        return crs.equalsIgnoreCase(that.crs) && algorithm == that.algorithm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), crs.toUpperCase(Locale.ROOT), algorithm);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
