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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

/** A string label bound directly to one entity in a catalog. */
@Experimental
public class Label {

    private final String entityType;
    private final String entityName;
    private final String key;
    private final String value;

    public Label(String entityType, String entityName, String key, String value) {
        this.entityType = entityType;
        this.entityName = entityName;
        this.key = key;
        this.value = value;
    }

    /** Returns the extensible, server-defined entity type. */
    public String getEntityType() {
        return entityType;
    }

    /** Returns the server's canonical entity name within the catalog. */
    public String getEntityName() {
        return entityName;
    }

    /** Returns the case-sensitive label key. */
    public String getKey() {
        return key;
    }

    /** Returns the label value, which may be an empty string. */
    public String getValue() {
        return value;
    }
}
