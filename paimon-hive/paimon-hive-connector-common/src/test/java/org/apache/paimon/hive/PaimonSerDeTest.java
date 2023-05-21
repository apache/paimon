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

package org.apache.paimon.hive;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.objectinspector.PaimonInternalRowObjectInspector;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_COMMENTS;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_NAMES;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.ROW_TYPE;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.generate;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonSerDe}. */
public class PaimonSerDeTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testInitialize() throws Exception {
        PaimonSerDe serDe = createInitializedSerDe();
        ObjectInspector o = serDe.getObjectInspector();
        assertThat(o).isInstanceOf(PaimonInternalRowObjectInspector.class);
        PaimonInternalRowObjectInspector oi = (PaimonInternalRowObjectInspector) o;
        GenericRow rowData = generate();
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        for (int i = 0; i < structFields.size(); i++) {
            assertThat(oi.getStructFieldData(rowData, structFields.get(i)))
                    .isEqualTo(rowData.getField(i));
            assertThat(structFields.get(i).getFieldName()).isEqualTo(FIELD_NAMES.get(i));
            assertThat(structFields.get(i).getFieldComment()).isEqualTo(FIELD_COMMENTS.get(i));
        }
    }

    @Test
    public void testDeserialize() throws Exception {
        PaimonSerDe serDe = createInitializedSerDe();
        GenericRow rowData = generate();
        RowDataContainer container = new RowDataContainer();
        container.set(rowData);
        assertThat(serDe.deserialize(container)).isEqualTo(rowData);
    }

    private PaimonSerDe createInitializedSerDe() throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()))
                .createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));

        Properties properties = new Properties();
        properties.setProperty("location", tempDir.toString());

        PaimonSerDe serDe = new PaimonSerDe();
        serDe.initialize(null, properties);
        return serDe;
    }
}
