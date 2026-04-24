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

package org.apache.paimon.format.orc;

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.base.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.format.orc.OrcFileFormat.refineDataType;
import static org.apache.paimon.format.orc.OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY;
import static org.apache.paimon.format.orc.OrcTypeUtil.convertToOrcSchema;
import static org.apache.paimon.format.orc.OrcTypeUtil.convertToOrcType;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Test for {@link OrcTypeUtil}. */
class OrcTypeUtilTest {

    @Test
    void testDataTypeToOrcType() {
        test("boolean", DataTypes.BOOLEAN());
        test("char(123)", DataTypes.CHAR(123));
        test("varchar(123)", DataTypes.VARCHAR(123));
        test("string", DataTypes.STRING());
        test("binary", DataTypes.BYTES());
        test("tinyint", DataTypes.TINYINT());
        test("smallint", DataTypes.SMALLINT());
        test("int", DataTypes.INT());
        test("bigint", DataTypes.BIGINT());
        test("float", DataTypes.FLOAT());
        test("double", DataTypes.DOUBLE());
        test("date", DataTypes.DATE());
        test("timestamp", DataTypes.TIMESTAMP());
        test("array<float>", DataTypes.ARRAY(DataTypes.FLOAT()));
        test("map<float,bigint>", DataTypes.MAP(DataTypes.FLOAT(), DataTypes.BIGINT()));
        test(
                "struct<int0:int,str1:string,double2:double,row3:struct<int0:int,int1:int>>",
                DataTypes.ROW(
                        DataTypes.FIELD(0, "int0", DataTypes.INT()),
                        DataTypes.FIELD(1, "str1", DataTypes.STRING()),
                        DataTypes.FIELD(2, "double2", DataTypes.DOUBLE()),
                        DataTypes.FIELD(
                                3,
                                "row3",
                                DataTypes.ROW(
                                        DataTypes.FIELD(4, "int0", DataTypes.INT()),
                                        DataTypes.FIELD(5, "int1", DataTypes.INT())))));
        test("decimal(4,2)", DataTypes.DECIMAL(4, 2));
    }

    private void test(String expected, DataType type) {
        assertThat(convertToOrcType(type, -1, -1)).hasToString(expected);
    }

    @Test
    void testFieldIdAttribute(@TempDir java.nio.file.Path tempPath) throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field(
                                "b",
                                RowType.builder(true, new AtomicInteger(10))
                                        .field("f0", DataTypes.STRING())
                                        .field("f1", DataTypes.INT())
                                        .build())
                        .field("c", DataTypes.ARRAY(DataTypes.INT()))
                        .field("d", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .field(
                                "e",
                                DataTypes.ARRAY(
                                        RowType.builder(true, new AtomicInteger(20))
                                                .field("f0", DataTypes.STRING())
                                                .field("f1", DataTypes.INT())
                                                .build()))
                        .field(
                                "f",
                                RowType.builder(true, new AtomicInteger(30))
                                        .field("f0", DataTypes.ARRAY(DataTypes.INT()))
                                        .build())
                        .build();

        // write schema to orc file then get
        FileIO fileIO = LocalFileIO.create();
        Path tempFile = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());

        OrcFileFormat format =
                new OrcFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        PositionOutputStream out = fileIO.newOutputStream(tempFile, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.close();
        out.close();

        Reader orcReader =
                OrcReaderFactory.createReader(new Configuration(), fileIO, tempFile, null);
        TypeDescription orcSchema = orcReader.getSchema();

        RowType refined = (RowType) refineDataType(rowType);

        assertThatNoException()
                .isThrownBy(() -> checkStruct(convertToOrcSchema(refined), orcSchema));

        assertThatNoException()
                .isThrownBy(
                        () ->
                                checkStruct(
                                        convertToOrcSchema(refined.project("c", "b", "d")),
                                        orcSchema));

        assertThatNoException()
                .isThrownBy(
                        () ->
                                checkStruct(
                                        convertToOrcSchema(refined.project("a", "e", "f")),
                                        orcSchema));
    }

    private void checkStruct(TypeDescription requiredStruct, TypeDescription orcStruct) {
        List<String> requiredFields = requiredStruct.getFieldNames();
        List<TypeDescription> requiredTypes = requiredStruct.getChildren();
        List<String> orcFields = orcStruct.getFieldNames();
        List<TypeDescription> orcTypes = orcStruct.getChildren();

        for (int i = 0; i < requiredFields.size(); i++) {
            String field = requiredFields.get(i);
            int orcIndex = orcFields.indexOf(field);
            checkArgument(orcIndex != -1, "Cannot find field %s in orc file meta.", field);
            TypeDescription requiredType = requiredTypes.get(i);
            TypeDescription orcType = orcTypes.get(orcIndex);
            checkField(field, requiredType, orcType);
        }
    }

    private void checkField(
            String fieldName, TypeDescription requiredType, TypeDescription orcType) {
        checkFieldIdAttribute(fieldName, requiredType, orcType);
        if (requiredType.getCategory().isPrimitive()) {
            return;
        }

        switch (requiredType.getCategory()) {
            case LIST:
                checkField(
                        "_elem", requiredType.getChildren().get(0), orcType.getChildren().get(0));
                return;
            case MAP:
                checkField("_key", requiredType.getChildren().get(0), orcType.getChildren().get(0));
                checkField(
                        "_value", requiredType.getChildren().get(1), orcType.getChildren().get(1));
                return;
            case STRUCT:
                checkStruct(requiredType, orcType);
                return;
            default:
                throw new UnsupportedOperationException("Unsupported orc type: " + requiredType);
        }
    }

    private void checkFieldIdAttribute(
            String fieldName, TypeDescription requiredType, TypeDescription orcType) {
        String requiredId = requiredType.getAttributeValue(PAIMON_ORC_FIELD_ID_KEY);
        String orcId = orcType.getAttributeValue(PAIMON_ORC_FIELD_ID_KEY);
        checkArgument(
                Objects.equal(requiredId, orcId),
                "Field %s has different id: read type id is %s but orc type id is %s. This is unexpected.",
                fieldName,
                requiredId,
                orcId);
    }
}
