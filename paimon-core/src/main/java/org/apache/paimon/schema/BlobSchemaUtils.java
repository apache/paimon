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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.FallbackKey;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;

/** Utilities for BLOB-related schema evolution (ALTER TABLE ADD COLUMN comment directives). */
public final class BlobSchemaUtils {

    public static final String BLOB_FIELD_DIRECTIVE = "__BLOB_FIELD";
    public static final String BLOB_DESCRIPTOR_FIELD_DIRECTIVE = "__BLOB_DESCRIPTOR_FIELD";

    private BlobSchemaUtils() {}

    /**
     * Parses the comment of an {@code ALTER TABLE ADD COLUMN} statement. Returns {@code null} when
     * the comment is a regular user comment; returns a {@link ParsedDirective} when the comment
     * begins with a supported BLOB directive. Throws {@link IllegalArgumentException} when the
     * comment begins with {@code __BLOB} but is not one of the supported directives.
     */
    @Nullable
    public static ParsedDirective parseAddColumnComment(@Nullable String comment) {
        if (comment == null || !comment.startsWith("__BLOB")) {
            return null;
        }
        comment = StringUtils.trim(comment);
        String optionKey = matchDirective(comment, BLOB_DESCRIPTOR_FIELD_DIRECTIVE);
        String marker = BLOB_DESCRIPTOR_FIELD_DIRECTIVE;
        if (optionKey == null) {
            optionKey = matchDirective(comment, BLOB_FIELD_DIRECTIVE);
            marker = BLOB_FIELD_DIRECTIVE;
        }
        Preconditions.checkArgument(
                optionKey != null,
                "Unsupported BLOB directive in column comment: '%s'. Supported directives are "
                        + "'%s' and '%s'.",
                comment,
                BLOB_FIELD_DIRECTIVE,
                BLOB_DESCRIPTOR_FIELD_DIRECTIVE);
        String realComment =
                comment.length() == marker.length()
                        ? null
                        : comment.substring(marker.length() + 1).trim();
        if (realComment != null && realComment.isEmpty()) {
            realComment = null;
        }
        return new ParsedDirective(optionKey, realComment);
    }

    @Nullable
    private static String matchDirective(String comment, String marker) {
        if (!comment.startsWith(marker)) {
            return null;
        }
        if (comment.length() == marker.length()) {
            return optionKeyFor(marker);
        }
        return comment.charAt(marker.length()) == ';' ? optionKeyFor(marker) : null;
    }

    private static String optionKeyFor(String marker) {
        if (BLOB_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_FIELD.key();
        } else if (BLOB_DESCRIPTOR_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_DESCRIPTOR_FIELD.key();
        } else {
            throw new IllegalArgumentException("Unsupported BLOB directive: " + marker);
        }
    }

    /**
     * Modify blob options, ensure the `blob-field`, `blob-descriptor-field` is consistent with
     * actual schema. If the canonical key is empty but a fallback key holds the value (e.g. legacy
     * {@code blob.stored-descriptor-fields}), the fallback value is migrated to the canonical key
     * before appending so old entries are not shadowed.
     */
    public static void modifyBlobOptions(
            String blobKey, String fieldName, Map<String, String> options) {
        ConfigOption<String> option;
        if (CoreOptions.BLOB_FIELD.key().equals(blobKey)) {
            option = CoreOptions.BLOB_FIELD;
        } else if (CoreOptions.BLOB_DESCRIPTOR_FIELD.key().equals(blobKey)) {
            option = CoreOptions.BLOB_DESCRIPTOR_FIELD;
        } else {
            throw new IllegalArgumentException("Unsupported BLOB directive: " + blobKey);
        }

        String existing = options.get(blobKey);
        if (existing == null || existing.isEmpty()) {
            // migrate legacy fallback keys to current canonical key
            for (FallbackKey fk : option.fallbackKeys()) {
                String fallbackValue = options.remove(fk.getKey());
                if (fallbackValue != null && !fallbackValue.isEmpty()) {
                    existing = fallbackValue;
                    break;
                }
            }
        }
        String newValue = existing == null ? fieldName : existing + "," + fieldName;
        options.put(blobKey, newValue);
    }

    /** Parsed BLOB directive: the option key to update and the user-facing comment. */
    public static final class ParsedDirective {
        private final String optionKey;
        @Nullable private final String realComment;

        private ParsedDirective(String optionKey, @Nullable String realComment) {
            this.optionKey = optionKey;
            this.realComment = realComment;
        }

        public String optionKey() {
            return optionKey;
        }

        @Nullable
        public String realComment() {
            return realComment;
        }
    }
}
