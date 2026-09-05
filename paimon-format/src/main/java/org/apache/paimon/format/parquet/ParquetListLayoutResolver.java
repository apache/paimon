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

package org.apache.paimon.format.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.LIST_ELEMENT_NAME;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves Parquet list layouts, following the backward-compatibility rules in the Parquet spec: <a
 * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules">LogicalTypes#Backward-compatibility-rules</a>.
 *
 * <p>All list layout decisions should be made through this class so that schema inference,
 * requested-schema clipping and reader construction share a single interpretation.
 */
public final class ParquetListLayoutResolver {

    private static final String LIST_WRAPPER_NAME = "list";
    private static final String LEGACY_LIST_ARRAY_NAME = "array";

    private ParquetListLayoutResolver() {}

    /** Returns true if the given group is annotated as a Parquet LIST logical type. */
    public static boolean isList(GroupType listType) {
        return listType.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }

    /**
     * Whether the given group has the legacy nested-list shape: an unannotated {@code REPEATED}
     * group whose single child is also {@code REPEATED}.
     *
     * <p>This is the Parquet spec's backward-compatibility <b>Rule 3</b> and appears as the element
     * of an annotated list, e.g. {@code repeated group element { repeated int32 array; }}. The
     * repeated child is the element of the nested list.
     */
    public static boolean isLegacyNestedList(GroupType groupType) {
        return groupType.isRepetition(Type.Repetition.REPEATED)
                && groupType.getFieldCount() == 1
                && groupType.getType(0).getRepetition() == Type.Repetition.REPEATED;
    }

    /**
     * Returns true if the given group is a three-level Parquet list.
     *
     * <p>In a three-level list the immediate repeated child is a wrapper group whose single
     * non-repeated child is the actual element type. This covers the canonical layout ({@code list
     * -> element}) as well as legacy wrappers such as Hive's {@code bag} layout.
     *
     * <p>This corresponds to the Parquet spec's backward-compatibility <b>Rule 5</b>: a repeated
     * group that contains exactly one non-repeated child is a wrapper, unless it matches one of
     * Rules 1-4.
     *
     * <p>The compatibility encodings that are <em>not</em> three-level are:
     *
     * <ul>
     *   <li><b>Rule 1</b>: the repeated field is a primitive and is itself the element type.
     *   <li><b>Rule 2</b>: the repeated field is a group with multiple fields and is itself the
     *       element type.
     *   <li><b>Rule 3</b>: the repeated field is a group whose single child is also repeated; the
     *       group itself is the element type.
     *   <li><b>Rule 4</b>: the repeated field is a group named {@code "array"} or {@code
     *       "<list>_tuple"} with a single child; the group itself is the element type.
     * </ul>
     */
    public static boolean isThreeLevelList(Type type) {
        if (type.isPrimitive()) {
            return false;
        }

        GroupType listType = type.asGroupType();
        if (!isList(listType)) {
            return false;
        }

        // A list must have exactly one repeated child (the middle level).
        if (listType.getFieldCount() != 1) {
            return false;
        }
        Type middle = listType.getType(0);
        if (middle.isPrimitive() || middle.getRepetition() != Type.Repetition.REPEATED) {
            return false;
        }
        GroupType repeatedGroup = middle.asGroupType();

        // Rule 5: the repeated group is a wrapper containing exactly one non-repeated child.
        if (repeatedGroup.getFieldCount() != 1
                || repeatedGroup.getType(0).getRepetition() == Type.Repetition.REPEATED) {
            return false;
        }

        // Rule 4: legacy "array" and "<list>_tuple" encodings are not wrappers; the repeated
        // group itself is the element type.
        return !LEGACY_LIST_ARRAY_NAME.equals(repeatedGroup.getName())
                && !(listType.getName() + "_tuple").equals(repeatedGroup.getName());
    }

    /**
     * Returns true if the given group follows the canonical three-level Parquet list layout ({@code
     * list -> element}).
     *
     * <p>The canonical layout is described in the Parquet spec: <a
     * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">LogicalTypes#Lists</a>
     */
    public static boolean isCanonicalList(Type type) {
        if (!isThreeLevelList(type)) {
            return false;
        }

        Type middle = type.asGroupType().getType(0);
        Type element = middle.asGroupType().getType(0);
        return LIST_WRAPPER_NAME.equals(middle.getName())
                && LIST_ELEMENT_NAME.equals(element.getName());
    }

    /**
     * Resolves the element type of the given LIST-annotated group according to the Parquet spec's
     * backward-compatibility rules for lists.
     *
     * <p>For a three-level list (Rule 5) the returned type is the single child of the repeated
     * wrapper. For Rules 1-4 the repeated field itself is returned because it is the element type.
     */
    public static Type resolveElementType(GroupType listType) {
        checkArgument(
                isList(listType) || isLegacyNestedList(listType),
                "Expected LIST-annotated group but got: %s",
                listType);

        if (isThreeLevelList(listType)) {
            return listType.getType(0).asGroupType().getType(0);
        }

        return listType.getType(0);
    }

    /**
     * A schema-level manifest of list layouts, modeled after parquet-cpp's {@code SchemaManifest}.
     *
     * <p>Built once from the file schema, it resolves the three-level verdict of every
     * LIST-annotated node in the file and records it by field path, so that reader construction
     * interprets list layouts against the file schema rather than the (possibly reshaped) requested
     * schema. Field paths are the identifier shared by the file schema, the requested schema and
     * the ColumnIO tree: clipping may rebuild nodes, but it preserves names, so paths stay stable
     * where node identity would not.
     *
     * <p>Paths absent from this context belong to nodes that are not LIST-annotated in the file
     * schema (or synthetic fill fields, whose requested shape is Paimon-canonical); the verdict
     * falls back to interpreting the requested node itself.
     */
    public static final class LayoutContext {
        private final Map<List<String>, Boolean> threeLevelMapping = new HashMap<>();

        /** Builds the manifest by resolving every LIST-annotated node of the file schema. */
        public static LayoutContext fromFileSchema(GroupType fileSchema) {
            LayoutContext context = new LayoutContext();
            for (Type field : fileSchema.getFields()) {
                collect(field, Collections.singletonList(field.getName()), context);
            }
            return context;
        }

        private static void collect(Type node, List<String> path, LayoutContext context) {
            if (node.isPrimitive()) {
                return;
            }
            GroupType group = node.asGroupType();
            if (isList(group)) {
                context.threeLevelMapping.put(
                        path, ParquetListLayoutResolver.isThreeLevelList(group));
            }
            for (Type child : group.getFields()) {
                List<String> childPath = new ArrayList<>(path);
                childPath.add(child.getName());
                collect(child, childPath, context);
            }
        }

        /**
         * Returns whether the list at {@code path} is a three-level list, as resolved against the
         * file schema; falls back to interpreting {@code requestedGroup} itself when the path is
         * not an annotated list in the file schema.
         */
        public boolean isThreeLevelList(GroupType requestedGroup, String[] path) {
            Boolean threeLevel = threeLevelMapping.get(Arrays.asList(path));
            return threeLevel != null
                    ? threeLevel
                    : ParquetListLayoutResolver.isThreeLevelList(requestedGroup);
        }
    }
}
