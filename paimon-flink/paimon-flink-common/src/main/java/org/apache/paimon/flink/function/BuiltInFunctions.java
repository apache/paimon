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

package org.apache.paimon.flink.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Paimon flink built in functions. */
public class BuiltInFunctions {

    /** Functions that can be instantiated by class name (no catalog context needed). */
    public static final Map<String, String> FUNCTIONS =
            new HashMap<String, String>() {
                {
                    put("path_to_descriptor", PathToDescriptor.class.getName());
                    put("descriptor_to_string", DescriptorToString.class.getName());
                }
            };

    /** Function names that require catalog options to instantiate. */
    public static final Set<String> CATALOG_AWARE_FUNCTIONS =
            new HashSet<String>() {
                {
                    add("blob_reference");
                }
            };

    /**
     * Creates a catalog-aware function instance with the given catalog options.
     *
     * @param name function name
     * @param catalogOptions catalog options
     * @return function instance
     */
    public static ScalarFunction createCatalogAwareFunction(
            String name, Map<String, String> catalogOptions) {
        switch (name) {
            case "blob_reference":
                return new BlobReferenceFunction(catalogOptions);
            default:
                throw new IllegalArgumentException("Unknown catalog-aware function: " + name);
        }
    }
}
