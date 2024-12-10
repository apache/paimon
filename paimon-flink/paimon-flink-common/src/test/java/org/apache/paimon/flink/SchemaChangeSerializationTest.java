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

package org.apache.paimon.flink;

import io.github.pixee.security.ObjectInputFilters;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serializing {@link SchemaChange}. */
public class SchemaChangeSerializationTest {

    @Test
    public void testSerialization() throws Exception {
        runTest(SchemaChange.setOption("key", "value"));
        runTest(SchemaChange.removeOption("key"));
        runTest(
                SchemaChange.addColumn(
                        "col", DataTypes.INT(), "comment", SchemaChange.Move.first("col")));
        runTest(SchemaChange.renameColumn("col", "new_col"));
        runTest(SchemaChange.dropColumn("col"));
        runTest(SchemaChange.updateColumnType("col", DataTypes.INT()));
        runTest(SchemaChange.updateColumnNullability(new String[] {"col1", "col2"}, true));
        runTest(SchemaChange.updateColumnComment(new String[] {"col1", "col2"}, "comment"));
        runTest(SchemaChange.updateColumnPosition(SchemaChange.Move.after("col", "ref")));
        runTest(SchemaChange.updateComment("comment"));
    }

    private void runTest(SchemaChange schemaChange) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(schemaChange);
        oos.close();
        byte[] bytes = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        ObjectInputFilters.enableObjectFilterIfUnprotected(ois);
        Object actual = ois.readObject();
        assertThat(actual).isEqualTo(schemaChange);
    }
}
