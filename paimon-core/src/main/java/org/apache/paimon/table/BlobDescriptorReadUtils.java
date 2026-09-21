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

package org.apache.paimon.table;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReaderFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Utilities for resolving BLOB descriptors during reads. */
public final class BlobDescriptorReadUtils {

    private BlobDescriptorReadUtils() {}

    public static UriReaderFactory createUriReaderFactory(
            Table table, int[] blobDescriptorFieldIndices) {
        if (blobDescriptorFieldIndices.length == 0) {
            return null;
        }

        return table instanceof FileStoreTable
                ? BlobDescriptorReaderFactory.create((FileStoreTable) table)
                : null;
    }

    public static int[] blobDescriptorFieldIndices(
            Table table, RowType readType, boolean blobAsDescriptor) {
        if (blobAsDescriptor || !(table instanceof FileStoreTable)) {
            return new int[0];
        }

        Set<String> blobDescriptorFields =
                ((FileStoreTable) table).coreOptions().blobDescriptorField();
        List<Integer> indices = new ArrayList<>();
        List<DataField> fields = readType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (blobDescriptorFields.contains(fields.get(i).name())) {
                indices.add(i);
            }
        }

        int[] result = new int[indices.size()];
        for (int i = 0; i < indices.size(); i++) {
            result[i] = indices.get(i);
        }
        return result;
    }
}
