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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.function.Function;

import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.extensions.UnResolvedPaimonV1Function;

/**
 * Catalog supports v1 function, i.e. functions surfaced through Spark's v1 (session / persistent)
 * function mechanism: file (Hive) functions and SQL functions.
 */
public interface SupportV1Function extends WithPaimonCatalog {

    boolean v1FunctionEnabled();

    /** Look up the function in the catalog. */
    Function getFunction(FunctionIdentifier funcIdent) throws Exception;

    /** Create a v1 function (file or SQL) from an already-built Paimon {@link Function}. */
    void createV1Function(Function function, boolean ignoreIfExists) throws Exception;

    boolean v1FunctionRegistered(FunctionIdentifier funcIdent);

    /** Resolve a v1 function reference (file or SQL) to an Expression. */
    Expression registerAndResolveV1Function(UnResolvedPaimonV1Function unresolvedV1Function)
            throws Exception;

    /** Unregister the func first, then drop it. */
    void dropV1Function(FunctionIdentifier funcIdent, boolean ifExists) throws Exception;
}
