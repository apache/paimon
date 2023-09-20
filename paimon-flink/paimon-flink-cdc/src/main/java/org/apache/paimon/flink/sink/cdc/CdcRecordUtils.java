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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utils for {@link CdcRecord}. */
public class CdcRecordUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CdcRecordUtils.class);

    /**
     * Project {@code fields} to a {@link GenericRow}. The fields of row are specified by the given
     * {@code dataFields} and its {@link RowKind} will always be {@link RowKind#INSERT}.
     *
     * <p>NOTE: This method will always return a {@link GenericRow} even if some keys of {@code
     * fields} are not in {@code dataFields}. If you want to make sure all field names of {@code
     * dataFields} existed in keys of {@code fields}, use {@link CdcRecordUtils#toGenericRow}
     * instead.
     *
     * @param dataFields {@link DataField}s of the converted {@link GenericRow}.
     * @return the projected {@link GenericRow}.
     */
    public static GenericRow projectAsInsert(CdcRecord record, List<DataField> dataFields) {
        GenericRow genericRow = new GenericRow(dataFields.size());
        for (int i = 0; i < dataFields.size(); i++) {
            DataField dataField = dataFields.get(i);
            String fieldValue = record.fields().get(dataField.name());
            if (!StringUtils.isEmpty(fieldValue)) {
                genericRow.setField(
                        i, TypeUtils.castFromCdcValueString(fieldValue, dataField.type()));
            }
        }
        return genericRow;
    }

    /**
     * Convert {@code fields} to a {@link GenericRow}. The fields of row are specified by the given
     * {@code dataFields} and its {@link RowKind} is determined by {@code kind} of this {@link
     * CdcRecord}.
     *
     * <p>NOTE: This method requires all field names of {@code dataFields} existed in keys of {@code
     * fields}. If you only want to convert some {@code fields}, use {@link
     * CdcRecordUtils#projectAsInsert} instead.
     *
     * @param dataFields {@link DataField}s of the converted {@link GenericRow}.
     * @return if all field names of {@code dataFields} existed in keys of {@code fields} and all
     *     values of {@code fields} can be correctly converted to the specified type, an {@code
     *     Optional#of(GenericRow)} will be returned, otherwise an {@code Optional#empty()} will be
     *     returned
     */
    public static Optional<GenericRow> toGenericRow(CdcRecord record, List<DataField> dataFields) {
        GenericRow genericRow = new GenericRow(record.kind(), dataFields.size());
        List<String> fieldNames =
                dataFields.stream().map(DataField::name).collect(Collectors.toList());

        for (Map.Entry<String, String> field : record.fields().entrySet()) {
            String key = field.getKey();
            String value = field.getValue();

            int idx = fieldNames.indexOf(key);
            if (idx < 0) {
                LOG.info("Field " + key + " not found. Waiting for schema update.");
                return Optional.empty();
            }

            if (value == null) {
                continue;
            }

            DataType type = dataFields.get(idx).type();
            // TODO TypeUtils.castFromString cannot deal with complex types like arrays and
            //  maps. Change type of CdcRecord#field if needed.
            try {
                genericRow.setField(idx, TypeUtils.castFromCdcValueString(value, type));
            } catch (Exception e) {
                LOG.info(
                        "Failed to convert value "
                                + value
                                + " to type "
                                + type
                                + ". Waiting for schema update.",
                        e);
                return Optional.empty();
            }
        }
        return Optional.of(genericRow);
    }

    public static CdcRecord fromGenericRow(GenericRow row, List<String> fieldNames) {
        Map<String, String> fields = new HashMap<>();
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object field = row.getField(i);
            if (field != null) {
                fields.put(fieldNames.get(i), field.toString());
            }
        }

        return new CdcRecord(row.getRowKind(), fields);
    }
}
