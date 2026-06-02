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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ColumnDirectiveUtils}. */
public class ColumnDirectiveUtilsTest {

    // -- applyAddColumnDirective (single-column, the main API for ADD COLUMN) --

    @Test
    public void testNonDirectiveCommentReturnsNull() {
        Map<String, String> opts = new HashMap<>();
        assertThat(
                        ColumnDirectiveUtils.applyAddColumnDirective(
                                null, "col", DataTypes.BYTES(), opts))
                .isNull();
        assertThat(ColumnDirectiveUtils.applyAddColumnDirective("", "col", DataTypes.BYTES(), opts))
                .isNull();
        assertThat(
                        ColumnDirectiveUtils.applyAddColumnDirective(
                                "normal comment", "col", DataTypes.BYTES(), opts))
                .isNull();
        assertThat(opts).isEmpty();
    }

    @Test
    public void testBlobFieldDirective() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_FIELD; profile picture", "pic", DataTypes.BYTES(), opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
        assertThat(result.comment()).isEqualTo("profile picture");
        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "pic");
    }

    @Test
    public void testBlobDescriptorFieldDirective() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_DESCRIPTOR_FIELD; desc text", "desc_col", DataTypes.BYTES(), opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
        assertThat(result.comment()).isEqualTo("desc text");
        assertThat(opts).containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "desc_col");
    }

    @Test
    public void testBlobViewFieldDirective() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_VIEW_FIELD; view comment", "view_col", DataTypes.BYTES(), opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
        assertThat(result.comment()).isEqualTo("view comment");
        assertThat(opts).containsEntry(CoreOptions.BLOB_VIEW_FIELD.key(), "view_col");
    }

    @Test
    public void testBlobExternalStorageFieldDirective() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_EXTERNAL_STORAGE_FIELD; external video",
                        "video",
                        DataTypes.BYTES(),
                        opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
        assertThat(result.comment()).isEqualTo("external video");
        assertThat(opts).containsEntry(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "video");
        assertThat(opts).containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "video");
    }

    @Test
    public void testVectorFieldDirective() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__VECTOR_FIELD;128; embedding vector",
                        "emb",
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.VECTOR);
        VectorType vectorType = (VectorType) result.type();
        assertThat(vectorType.getLength()).isEqualTo(128);
        assertThat(vectorType.getElementType()).isEqualTo(DataTypes.FLOAT());
        assertThat(result.comment()).isEqualTo("embedding vector");
        assertThat(opts).containsEntry(CoreOptions.VECTOR_FIELD.key(), "emb");
    }

    @Test
    public void testVectorFieldDirectiveWithoutComment() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__VECTOR_FIELD;64", "emb", DataTypes.ARRAY(DataTypes.DOUBLE()), opts);

        assertThat(result).isNotNull();
        VectorType vectorType = (VectorType) result.type();
        assertThat(vectorType.getLength()).isEqualTo(64);
        assertThat(vectorType.getElementType()).isEqualTo(DataTypes.DOUBLE());
        assertThat(result.comment()).isNull();
    }

    @Test
    public void testBlobDirectiveAppendsToExistingOption() {
        Map<String, String> opts = new HashMap<>();
        opts.put(CoreOptions.BLOB_FIELD.key(), "existing");

        ColumnDirectiveUtils.applyAddColumnDirective(
                "__BLOB_FIELD", "new_col", DataTypes.BYTES(), opts);

        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "existing,new_col");
    }

    @Test
    public void testBareDirectiveWithoutComment() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_FIELD", "col", DataTypes.BYTES(), opts);

        assertThat(result).isNotNull();
        assertThat(result.comment()).isNull();
    }

    @Test
    public void testBlobDirectiveWithBlobSourceType() {
        Map<String, String> opts = new HashMap<>();
        ColumnDirectiveUtils.ConvertedColumn result =
                ColumnDirectiveUtils.applyAddColumnDirective(
                        "__BLOB_FIELD", "col", DataTypes.BLOB(), opts);

        assertThat(result).isNotNull();
        assertThat(result.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
    }

    // -- applyAddColumnDirective error cases --

    @Test
    public void testBlobDirectiveRejectsNonBinaryType() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__BLOB_FIELD", "col", DataTypes.INT(), new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be of BYTES, BINARY or BLOB type");
    }

    @Test
    public void testVectorDirectiveRejectsNonArrayType() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__VECTOR_FIELD;128",
                                        "col",
                                        DataTypes.INT(),
                                        new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be of ARRAY type");
    }

    @Test
    public void testVectorDirectiveRequiresDimension() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__VECTOR_FIELD",
                                        "col",
                                        DataTypes.ARRAY(DataTypes.FLOAT()),
                                        new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a dimension");
    }

    @Test
    public void testVectorDirectiveRejectsNonIntegerDimension() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__VECTOR_FIELD;abc",
                                        "col",
                                        DataTypes.ARRAY(DataTypes.FLOAT()),
                                        new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected an integer dimension");
    }

    @Test
    public void testUnknownBlobDirectiveRejected() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__BLOB_UNKNOWN",
                                        "col",
                                        DataTypes.BYTES(),
                                        new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported BLOB directive");
    }

    @Test
    public void testUnknownVectorDirectiveRejected() {
        assertThatThrownBy(
                        () ->
                                ColumnDirectiveUtils.applyAddColumnDirective(
                                        "__VECTOR_UNKNOWN",
                                        "col",
                                        DataTypes.ARRAY(DataTypes.FLOAT()),
                                        new HashMap<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported VECTOR directive");
    }

    // -- applyDirectives (Schema-level, used by CREATE TABLE) --

    @Test
    public void testApplyDirectivesNoDirectives() {
        Schema original =
                new Schema(
                        RowType.of(
                                        new DataField[] {
                                            new DataField(0, "k", DataTypes.INT()),
                                            new DataField(1, "v", DataTypes.STRING())
                                        })
                                .getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");

        Schema result = ColumnDirectiveUtils.applyDirectives(original);
        assertThat(result).isSameAs(original);
    }

    @Test
    public void testApplyDirectivesMixedFields() {
        Map<String, String> options = new HashMap<>();
        Schema schema =
                new Schema(
                        RowType.of(
                                        new DataField[] {
                                            new DataField(0, "k", DataTypes.INT()),
                                            new DataField(
                                                    1,
                                                    "pic",
                                                    DataTypes.BYTES(),
                                                    "__BLOB_FIELD; picture"),
                                            new DataField(
                                                    2,
                                                    "emb",
                                                    DataTypes.ARRAY(DataTypes.FLOAT()),
                                                    "__VECTOR_FIELD;64; my embedding"),
                                            new DataField(
                                                    3, "normal", DataTypes.STRING(), "keep me")
                                        })
                                .getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        "");

        Schema result = ColumnDirectiveUtils.applyDirectives(schema);

        assertThat(result).isNotSameAs(schema);
        assertThat(result.fields()).hasSize(4);

        DataField pic = result.fields().get(1);
        assertThat(pic.type().getTypeRoot()).isEqualTo(DataTypeRoot.BLOB);
        assertThat(pic.description()).isEqualTo("picture");

        DataField emb = result.fields().get(2);
        assertThat(emb.type().getTypeRoot()).isEqualTo(DataTypeRoot.VECTOR);
        assertThat(emb.description()).isEqualTo("my embedding");

        DataField normal = result.fields().get(3);
        assertThat(normal.type()).isEqualTo(DataTypes.STRING());
        assertThat(normal.description()).isEqualTo("keep me");

        assertThat(result.options()).containsEntry(CoreOptions.BLOB_FIELD.key(), "pic");
        assertThat(result.options()).containsEntry(CoreOptions.VECTOR_FIELD.key(), "emb");
    }

    // -- removeDroppedDirectiveOptions --

    @Test
    public void testRemoveDroppedBlobOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put(CoreOptions.BLOB_FIELD.key(), "a,b");
        opts.put(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "b,c");
        opts.put(CoreOptions.BLOB_VIEW_FIELD.key(), "b");
        opts.put(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "b");
        opts.put("blob.stored-descriptor-fields", "b,legacy");
        opts.put(CoreOptions.VECTOR_FIELD.key(), "v");

        ColumnDirectiveUtils.removeDroppedDirectiveOptions("b", DataTypeRoot.BLOB, opts);

        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "a");
        assertThat(opts).containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "c");
        assertThat(opts).doesNotContainKey(CoreOptions.BLOB_VIEW_FIELD.key());
        assertThat(opts).doesNotContainKey(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key());
        assertThat(opts).containsEntry("blob.stored-descriptor-fields", "legacy");
        assertThat(opts).containsEntry(CoreOptions.VECTOR_FIELD.key(), "v");
    }

    @Test
    public void testRemoveDroppedVectorOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put(CoreOptions.BLOB_FIELD.key(), "a");
        opts.put(CoreOptions.VECTOR_FIELD.key(), "emb,emb2");
        opts.put("field.emb.vector-dim", "128");

        ColumnDirectiveUtils.removeDroppedDirectiveOptions("emb", DataTypeRoot.VECTOR, opts);

        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "a");
        assertThat(opts).containsEntry(CoreOptions.VECTOR_FIELD.key(), "emb2");
        assertThat(opts).doesNotContainKey("field.emb.vector-dim");
    }

    @Test
    public void testRemoveDroppedNonDirectiveTypeIsNoop() {
        Map<String, String> opts = new HashMap<>();
        opts.put(CoreOptions.BLOB_FIELD.key(), "a");
        opts.put(CoreOptions.VECTOR_FIELD.key(), "v");

        ColumnDirectiveUtils.removeDroppedDirectiveOptions("x", DataTypeRoot.INTEGER, opts);

        assertThat(opts).containsEntry(CoreOptions.BLOB_FIELD.key(), "a");
        assertThat(opts).containsEntry(CoreOptions.VECTOR_FIELD.key(), "v");
    }
}
