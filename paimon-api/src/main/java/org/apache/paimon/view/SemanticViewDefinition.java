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

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Complete semantic model text. Format and dialect support are validated by the server. */
@Experimental
public class SemanticViewDefinition {

    /** Maximum UTF-8 content size accepted by the semantic view REST contract. */
    public static final int MAX_CONTENT_BYTES = 1024 * 1024;

    private final String format;
    private final String dialect;
    private final String content;

    @JsonCreator
    @ConstructorProperties({"format", "dialect", "content"})
    public SemanticViewDefinition(
            @JsonProperty("format") String format,
            @JsonProperty("dialect") String dialect,
            @JsonProperty("content") String content) {
        checkArgument(format != null && !format.trim().isEmpty(), "format must not be blank");
        checkArgument(dialect != null && !dialect.trim().isEmpty(), "dialect must not be blank");
        checkArgument(content != null && !content.trim().isEmpty(), "content must not be blank");
        checkArgument(
                content.getBytes(StandardCharsets.UTF_8).length <= MAX_CONTENT_BYTES,
                "content must not exceed 1 MiB in UTF-8");
        this.format = format;
        this.dialect = dialect;
        this.content = content;
    }

    @JsonGetter("format")
    public String getFormat() {
        return format;
    }

    @JsonGetter("dialect")
    public String getDialect() {
        return dialect;
    }

    /** Returns the original document without parsing or normalization. */
    @JsonGetter("content")
    public String getContent() {
        return content;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SemanticViewDefinition)) {
            return false;
        }
        SemanticViewDefinition that = (SemanticViewDefinition) obj;
        return format.equals(that.format)
                && dialect.equals(that.dialect)
                && content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, dialect, content);
    }
}
