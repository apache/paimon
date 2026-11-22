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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Exact planar intersection of a GEOMETRY field with a constant WKB geometry in its CRS. */
public final class GeometryIntersectsTransform implements Transform {

    private static final long serialVersionUID = 1L;
    public static final String NAME = "ST_INTERSECTS";

    @JsonProperty("field")
    private final FieldRef field;

    @JsonProperty("wkb")
    private final byte[] wkb;

    private transient volatile PreparedGeometry prepared;

    @JsonCreator
    public GeometryIntersectsTransform(
            @JsonProperty("field") FieldRef field, @JsonProperty("wkb") byte[] wkb) {
        checkArgument(field.type().is(DataTypeRoot.GEOMETRY), "ST_Intersects requires GEOMETRY.");
        this.field = field;
        this.wkb = wkb.clone();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    @JsonIgnore
    public List<Object> inputs() {
        return Collections.singletonList(field);
    }

    @Override
    public DataType outputType() {
        return DataTypes.BOOLEAN();
    }

    @Override
    public Object transform(InternalRow row) {
        if (row.isNullAt(field.index())) {
            return null;
        }
        WKBReader reader = new WKBReader();
        try {
            PreparedGeometry query = preparedQuery(reader);
            Geometry value = reader.read(row.getBinary(field.index()));
            return query.intersects(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid WKB in ST_Intersects.", e);
        }
    }

    private PreparedGeometry preparedQuery(WKBReader reader) throws ParseException {
        PreparedGeometry query = prepared;
        if (query == null) {
            query = PreparedGeometryFactory.prepare(reader.read(wkb));
            prepared = query;
        }
        return query;
    }

    /**
     * Index candidates for a positive intersection; data readers evaluate the original transform.
     */
    public Optional<LeafPredicate> envelopeFilter() {
        try {
            Envelope box = preparedQuery(new WKBReader()).getGeometry().getEnvelopeInternal();
            if (box.isNull()) {
                return Optional.empty();
            }
            return Optional.of(
                    new LeafPredicate(
                            BBoxIntersects.INSTANCE,
                            field.type(),
                            field.index(),
                            field.name(),
                            Arrays.asList(
                                    box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY())));
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid WKB in ST_Intersects.", e);
        }
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        checkArgument(inputs.size() == 1);
        return new GeometryIntersectsTransform((FieldRef) inputs.get(0), wkb);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GeometryIntersectsTransform)) {
            return false;
        }
        GeometryIntersectsTransform that = (GeometryIntersectsTransform) other;
        return field.equals(that.field) && Arrays.equals(wkb, that.wkb);
    }

    @Override
    public int hashCode() {
        return 31 * field.hashCode() + Arrays.hashCode(wkb);
    }

    @Override
    public String toString() {
        return NAME + "(" + field.name() + ", WKB[" + wkb.length + "])";
    }
}
