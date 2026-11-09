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

import org.apache.paimon.annotation.Experimental;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Metadata for a model registered in a catalog database. */
@Experimental
public class Model {

    private final Identifier identifier;
    private final @Nullable String comment;
    private final ModelDefinition definition;

    public Model(Identifier identifier, @Nullable String comment, ModelDefinition definition) {
        checkArgument(identifier != null, "identifier must not be null");
        checkArgument(comment == null || !comment.trim().isEmpty(), "comment must not be blank");
        checkArgument(definition != null, "definition must not be null");
        this.identifier = identifier;
        this.comment = comment;
        this.definition = definition;
    }

    public Identifier identifier() {
        return identifier;
    }

    public String name() {
        return identifier.getObjectName();
    }

    public String fullName() {
        return identifier.getFullName();
    }

    @Nullable
    public String comment() {
        return comment;
    }

    public ModelDefinition definition() {
        return definition;
    }
}
