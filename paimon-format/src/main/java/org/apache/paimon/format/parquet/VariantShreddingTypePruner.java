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

import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.data.variant.VariantPathSegment;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Prunes a shredded Variant Parquet type according to a logical Variant projection.
 *
 * <p>It builds a trie from the requested object/array paths and recursively removes unneeded fields
 * from the {@code typed_value} (and list element) groups while preserving {@code value} fallbacks
 * when a requested path cannot be satisfied from typed columns.
 *
 * <p>Variant object keys are matched case-sensitively, independently of Parquet column-name
 * resolution, because Variant path extraction downstream resolves keys exactly through {@code
 * objectSchemaMap}.
 */
public class VariantShreddingTypePruner {
    private static final String LIST_WRAPPER_NAME = "list";
    private static final String LIST_ELEMENT_NAME = "element";

    @Nullable private final PathNode root;

    VariantShreddingTypePruner(RowType variantRowType) {
        this.root = buildPathTree(variantRowType);
    }

    /**
     * Clips the given Parquet Variant type to only include fields needed for {@code
     * variantRowType}.
     *
     * @param variantRowType the logical Variant projection row type
     * @param parquetType the physical Parquet Variant type
     * @return a clipped Parquet type
     */
    public static Type clip(RowType variantRowType, GroupType parquetType) {
        return new VariantShreddingTypePruner(variantRowType).clip(parquetType);
    }

    private Type clip(GroupType parquetType) {
        return clipShreddingRow(parquetType, root);
    }

    /** A projection trie for Variant object paths and array element paths. */
    private static class PathNode {
        private final Map<String, PathNode> children = new HashMap<>();
        private PathNode arrayElement;
        private boolean keepAll;

        private PathNode getOrCreateChild(String key) {
            return children.computeIfAbsent(key, k -> new PathNode());
        }
    }

    @Nullable
    private PathNode buildPathTree(RowType variantRowType) {
        PathNode root = new PathNode();
        for (DataField field : variantRowType.getFields()) {
            String path = VariantMetadataUtils.path(field.description());
            VariantPathSegment[] segments = VariantPathSegment.parse(path);
            if (segments.length == 0) {
                return null;
            }

            PathNode node = root;
            for (VariantPathSegment segment : segments) {
                if (segment instanceof VariantPathSegment.ArrayExtraction) {
                    // Array indices cannot prune individual elements at the Parquet level,
                    // but we can still prune nested fields inside each array element.
                    if (node.arrayElement == null) {
                        node.arrayElement = new PathNode();
                    }
                    node = node.arrayElement;
                } else if (segment instanceof VariantPathSegment.ObjectExtraction) {
                    String key = ((VariantPathSegment.ObjectExtraction) segment).getKey();
                    node = node.getOrCreateChild(key);
                } else {
                    return null;
                }
            }
            node.keepAll = true;
        }
        return root;
    }

    private Type clipShreddingRow(Type type, PathNode node) {
        if (type.isPrimitive() || node == null) {
            return type;
        }

        GroupType group = type.asGroupType();
        if (node.keepAll || !group.containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)) {
            return group;
        }

        List<Type> newFields = new ArrayList<>();
        if (group.containsField(PaimonShreddingUtils.METADATA_FIELD_NAME)) {
            newFields.add(group.getType(PaimonShreddingUtils.METADATA_FIELD_NAME));
        }

        Type typedValue = group.getType(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        if (isCanonicalList(typedValue) && node.arrayElement != null) {
            return clipListShreddingRow(group, node, newFields);
        } else if (isObjectGroup(typedValue) && node.arrayElement == null) {
            return clipObjectShreddingRow(group, node, newFields);
        } else {
            return group;
        }
    }

    private GroupType clipObjectShreddingRow(GroupType group, PathNode node, List<Type> newFields) {
        Type typedValueType = group.getType(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        GroupType typedValue = typedValueType.asGroupType();
        // typed_value is an object group: prune by object key.
        boolean needValue = false;
        List<Type> clippedTypedFields = new ArrayList<>();
        Set<String> requestedKeys = new HashSet<>(node.children.keySet());

        for (Type field : typedValue.getFields()) {
            String fieldName = field.getName();
            PathNode child = node.children.get(fieldName);
            if (child == null) {
                continue;
            }
            requestedKeys.remove(fieldName);

            checkArgument(!field.isPrimitive());
            Type clippedChild = clipShreddingRow(field, child);
            clippedTypedFields.add(clippedChild);
        }

        if (!requestedKeys.isEmpty()) {
            needValue = true;
        }

        if (needValue) {
            checkArgument(group.containsField(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
            newFields.add(group.getType(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
        }

        if (!clippedTypedFields.isEmpty()) {
            newFields.add(typedValue.withNewFields(clippedTypedFields));
        }
        return group.withNewFields(newFields);
    }

    private GroupType clipListShreddingRow(GroupType group, PathNode node, List<Type> newFields) {
        Type type = group.getType(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        GroupType listGroup = type.asGroupType();
        if (node.arrayElement.keepAll) {
            // The projection reads the whole array element (e.g. $.arr[0] read as VARIANT);
            return group;
        }

        // If there are also object projections on this Variant (e.g. querying both $[0].x and
        // $.y), the typed list columns cannot satisfy the object paths. We must keep the parent
        // value fallback so that object-shaped rows can still be read correctly.
        if (!node.children.isEmpty()) {
            checkArgument(group.containsField(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
            newFields.add(group.getType(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
        }

        Type elementType = parquetListElementType(listGroup);
        Type clippedElement = clipShreddingRow(elementType.asGroupType(), node.arrayElement);
        GroupType repeated = listGroup.getType(0).asGroupType();
        GroupType clippedRepeated =
                repeated.withNewFields(Collections.singletonList(clippedElement));
        newFields.add(listGroup.withNewFields(Collections.singletonList(clippedRepeated)));
        return group.withNewFields(newFields);
    }

    /**
     * Returns true if the given group follows the canonical three-level Parquet list layout.
     *
     * <p>The canonical layout is described in the Parquet spec: <a
     * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">LogicalTypes#Lists</a>
     */
    private static boolean isCanonicalList(Type type) {
        if (type.isPrimitive()) {
            return false;
        }

        GroupType listGroup = type.asGroupType();
        // 1. Must be a LIST logical type.
        if (!(listGroup.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)) {
            return false;
        }

        // 2. LIST group must have exactly one child named "list".
        if (listGroup.getFieldCount() != 1) {
            return false;
        }
        Type middle = listGroup.getType(0);
        if (!LIST_WRAPPER_NAME.equals(middle.getName())) {
            return false;
        }

        // 3. The child must be a repeated group.
        if (middle.isPrimitive() || middle.getRepetition() != Type.Repetition.REPEATED) {
            return false;
        }
        GroupType repeatedWrapper = middle.asGroupType();

        // 4. The repeated wrapper must contain exactly one child named "element".
        if (repeatedWrapper.getFieldCount() != 1) {
            return false;
        }

        Type element = repeatedWrapper.getType(0);
        return LIST_ELEMENT_NAME.equals(element.getName());
    }

    /** Returns true if the given group is a plain struct (not a Parquet list or map). */
    private static boolean isObjectGroup(Type type) {
        if (type.isPrimitive()) {
            return false;
        }
        GroupType groupType = type.asGroupType();
        return groupType.getLogicalTypeAnnotation() == null;
    }
}
