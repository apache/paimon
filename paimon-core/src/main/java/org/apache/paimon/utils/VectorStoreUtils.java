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

package org.apache.paimon.utils;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utils for vector-store table. */
public class VectorStoreUtils {
    public static boolean isDifferentFormat(FileFormat vectorStoreFormat, FileFormat normalFormat) {
        return (vectorStoreFormat != null)
                && !vectorStoreFormat
                        .getFormatIdentifier()
                        .equals(normalFormat.getFormatIdentifier());
    }

    public static boolean isVectorStoreFile(String fileName) {
        return fileName.contains(".vector.");
    }

    public static List<DataField> fieldsInVectorFile(
            RowType rowType, FileFormat normalFormat, FileFormat vectorStoreFormat) {
        if (!isDifferentFormat(vectorStoreFormat, normalFormat)) {
            return Collections.emptyList();
        }
        List<DataField> result = new ArrayList<>();
        rowType.getFields()
                .forEach(
                        field -> {
                            DataTypeRoot type = field.type().getTypeRoot();
                            if (type == DataTypeRoot.VECTOR) {
                                result.add(field);
                            }
                        });
        return result;
    }
}
