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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BlobSchemaUtils}. */
public class BlobSchemaUtilsTest {

    @Test
    public void testParseAddColumnComment() {
        // null and non-directive comments are passthrough.
        assertThat(BlobSchemaUtils.parseAddColumnComment(null)).isNull();
        assertThat(BlobSchemaUtils.parseAddColumnComment("")).isNull();
        assertThat(BlobSchemaUtils.parseAddColumnComment("normal user comment")).isNull();
        // case-sensitive: lowercase is not a directive.
        assertThat(BlobSchemaUtils.parseAddColumnComment("__blob_field; x")).isNull();

        // bare BLOB_FIELD directive
        BlobSchemaUtils.ParsedDirective bareBlob =
                BlobSchemaUtils.parseAddColumnComment(BlobSchemaUtils.BLOB_FIELD_DIRECTIVE);
        assertThat(bareBlob.optionKey()).isEqualTo(CoreOptions.BLOB_FIELD.key());
        assertThat(bareBlob.realComment()).isNull();

        // BLOB_FIELD with trailing semicolon only — still no real comment
        BlobSchemaUtils.ParsedDirective trailingSemi =
                BlobSchemaUtils.parseAddColumnComment(BlobSchemaUtils.BLOB_FIELD_DIRECTIVE + ";");
        assertThat(trailingSemi.realComment()).isNull();

        // BLOB_FIELD with real comment (note inner whitespace trimmed).
        BlobSchemaUtils.ParsedDirective withComment =
                BlobSchemaUtils.parseAddColumnComment(
                        BlobSchemaUtils.BLOB_FIELD_DIRECTIVE + ";   profile picture  ");
        assertThat(withComment.optionKey()).isEqualTo(CoreOptions.BLOB_FIELD.key());
        assertThat(withComment.realComment()).isEqualTo("profile picture");

        // BLOB_DESCRIPTOR_FIELD directive
        BlobSchemaUtils.ParsedDirective descriptor =
                BlobSchemaUtils.parseAddColumnComment(
                        BlobSchemaUtils.BLOB_DESCRIPTOR_FIELD_DIRECTIVE + "; desc text");
        assertThat(descriptor.optionKey()).isEqualTo(CoreOptions.BLOB_DESCRIPTOR_FIELD.key());
        assertThat(descriptor.realComment()).isEqualTo("desc text");
    }

    @Test
    public void testParseRejectsUnknownDirective() {
        assertThatThrownBy(() -> BlobSchemaUtils.parseAddColumnComment("__BLOB_VIEW_FIELD; x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported BLOB directive");
        assertThatThrownBy(() -> BlobSchemaUtils.parseAddColumnComment("__BLOB_UNKNOWN"))
                .isInstanceOf(IllegalArgumentException.class);
        // a __BLOB_FIELD prefix without a `;` boundary is not a valid directive.
        assertThatThrownBy(() -> BlobSchemaUtils.parseAddColumnComment("__BLOB_FIELDX"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testModifyBlobOptions() {
        Map<String, String> opts = new HashMap<>();
        BlobSchemaUtils.modifyBlobOptions(CoreOptions.BLOB_FIELD.key(), "a", opts);
        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "a");

        BlobSchemaUtils.modifyBlobOptions(CoreOptions.BLOB_FIELD.key(), "b", opts);
        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "a,b");

        BlobSchemaUtils.modifyBlobOptions(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "c", opts);
        assertThat(opts).containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "c");

        assertThatThrownBy(
                        () ->
                                BlobSchemaUtils.modifyBlobOptions(
                                        "not-a-blob-option", "x", new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported BLOB directive");
    }

    @Test
    public void testModifyBlobOptionsMigratesLegacyFallbackKey() {
        // legacy option holds the descriptor field; canonical key absent.
        Map<String, String> opts = new HashMap<>();
        opts.put("blob.stored-descriptor-fields", "legacy_col");

        BlobSchemaUtils.modifyBlobOptions(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "new_col", opts);

        // fallback value is migrated to canonical key, fallback key removed to avoid stale data.
        assertThat(opts)
                .containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "legacy_col,new_col");
        assertThat(opts).doesNotContainKey("blob.stored-descriptor-fields");
    }
}
