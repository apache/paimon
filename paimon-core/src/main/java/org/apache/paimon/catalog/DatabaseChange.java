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

package org.apache.paimon.catalog;

import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** define change to the database property. */
public interface DatabaseChange {

    static DatabaseChange setProperty(String property, String value) {
        return new SetProperty(property, value);
    }

    static DatabaseChange removeProperty(String property) {
        return new RemoveProperty(property);
    }

    static Pair<Map<String, String>, Set<String>> getAddPropertiesAndRemoveKeys(
            List<DatabaseChange> changes) {
        Map<String, String> insertProperties = Maps.newHashMap();
        Set<String> removeKeys = Sets.newHashSet();
        changes.forEach(
                change -> {
                    if (change instanceof DatabaseChange.SetProperty) {
                        DatabaseChange.SetProperty setProperty =
                                (DatabaseChange.SetProperty) change;
                        insertProperties.put(setProperty.property(), setProperty.value());
                    } else {
                        removeKeys.add(((DatabaseChange.RemoveProperty) change).property());
                    }
                });
        return Pair.of(insertProperties, removeKeys);
    }

    /** Set property for database change. */
    final class SetProperty implements DatabaseChange {

        private final String property;
        private final String value;

        private SetProperty(String property, String value) {
            this.property = property;
            this.value = value;
        }

        public String property() {
            return this.property;
        }

        public String value() {
            return this.value;
        }
    }

    /** Remove property for database change. */
    final class RemoveProperty implements DatabaseChange {

        private final String property;

        private RemoveProperty(String property) {
            this.property = property;
        }

        public String property() {
            return this.property;
        }
    }
}
