/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.format.orc;

import org.apache.orc.TypeDescription;

import java.util.List;

/** Visitor for ORC Schema to estimated record sizes. */
public class EstimateOrcAvgWidthVisitor extends OrcSchemaVisitor<Integer> {

    @Override
    public Integer record(TypeDescription record, List<String> names, List<Integer> fieldWidths) {
        return fieldWidths.stream().reduce(Integer::sum).orElse(0);
    }

    @Override
    public Integer list(TypeDescription array, Integer elementWidth) {
        return elementWidth;
    }

    @Override
    public Integer map(TypeDescription map, Integer keyWidth, Integer valueWidth) {
        return keyWidth + valueWidth;
    }

    @Override
    public Integer primitive(TypeDescription primitive) {
        switch (primitive.getCategory()) {
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case FLOAT:
            case BOOLEAN:
            case LONG:
            case DOUBLE:
            case DATE:
                return 8;
            case TIMESTAMP:
            case STRING:
            case VARCHAR:
            case BINARY:
                return 128;
            case DECIMAL:
                return primitive.getPrecision() + 2;
            default:
                throw new IllegalArgumentException("Can't handle " + primitive);
        }
    }
}
