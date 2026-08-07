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

package org.apache.paimon.manifest;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** Utilities for manifest schemas. */
final class ManifestSchemaUtils {

    static final String FORMAT_IDENTIFIER = "_VERSION";

    private ManifestSchemaUtils() {}

    /** Adds the permanent on-disk format identifier field to a manifest row type. */
    static RowType withFormatIdentifier(RowType rowType) {
        List<DataField> fields = new ArrayList<>();
        // Keep the historical field name for compatibility with existing manifest files.
        fields.add(new DataField(-1, FORMAT_IDENTIFIER, new IntType(false)));
        fields.addAll(rowType.getFields());
        return new RowType(false, fields);
    }
}
