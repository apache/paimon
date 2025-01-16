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

package org.apache.paimon.view;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Implementation of {@link View}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewImpl implements View {

    private final Identifier identifier;
    private final ViewSchema viewSchema;

    public ViewImpl(
            Identifier identifier,
            RowType rowType,
            String query,
            @Nullable String comment,
            Map<String, String> options) {
        this.identifier = identifier;
        this.viewSchema = new ViewSchema(query, comment, options, rowType);
    }

    @Override
    public String name() {
        return identifier.getObjectName();
    }

    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    @Override
    public RowType rowType() {
        return this.viewSchema.rowType();
    }

    @Override
    public String query() {
        return this.viewSchema.query();
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(this.viewSchema.comment());
    }

    @Override
    public Map<String, String> options() {
        return this.viewSchema.options();
    }

    @Override
    public View copy(Map<String, String> dynamicOptions) {
        Map<String, String> newOptions = new HashMap<>(options());
        newOptions.putAll(dynamicOptions);
        return new ViewImpl(identifier, rowType(), query(), this.viewSchema.comment(), newOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewImpl view = (ViewImpl) o;
        return Objects.equals(identifier, view.identifier)
                && Objects.equals(viewSchema, view.viewSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, viewSchema);
    }
}
