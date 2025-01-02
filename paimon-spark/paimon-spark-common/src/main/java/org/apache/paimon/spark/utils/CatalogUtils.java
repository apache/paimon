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

package org.apache.paimon.spark.utils;

import org.apache.spark.sql.connector.catalog.Identifier;

import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils of catalog. */
public class CatalogUtils {

    public static void checkNamespace(String[] namespace) {
        checkArgument(
                namespace.length == 1,
                "Paimon only support single namespace, but got %s",
                Arrays.toString(namespace));
    }

    public static org.apache.paimon.catalog.Identifier toIdentifier(Identifier ident) {
        checkNamespace(ident.namespace());
        return new org.apache.paimon.catalog.Identifier(ident.namespace()[0], ident.name());
    }
}
