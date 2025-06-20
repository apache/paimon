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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.RESTCatalogITCaseBase;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for function procedure. */
public class FunctionProcedureITCase extends RESTCatalogITCaseBase {

    @Test
    public void test() throws FunctionNotExistException {
        String functionName = "test_function";
        List<Row> result =
                sql(
                        String.format(
                                "CALL sys.create_function('%s.%s', "
                                        + "'[{\"id\": 0, \"name\":\"length\", \"type\":\"INT\"}, {\"id\": 1, \"name\":\"width\", \"type\":\"INT\"}]',"
                                        + "'[{\"id\": 0, \"name\":\"area\", \"type\":\"BIGINT\"}]', true, 'comment', 'k1=v1,k2=v2')",
                                DATABASE_NAME, functionName));
        assertThat(result.toString()).contains("Success");
        assertThat(batchSql(String.format("SHOW FUNCTIONS in %s", DATABASE_NAME)))
                .contains(Row.of(functionName));
        result =
                sql(
                        String.format(
                                "CALL sys.alter_function('%s.%s', '{\"action\" : \"addDefinition\", \"name\" : \"flink\", \"definition\" : {\"type\" : \"file\", \"fileResources\" : [{\"resourceType\": \"JAR\", \"uri\": \"oss://mybucket/xxxx.jar\"}], \"language\": \"JAVA\", \"className\": \"xxxx\", \"functionName\": \"functionName\" } }')",
                                DATABASE_NAME, functionName));
        assertThat(result.toString()).contains("Success");
        Catalog catalog = tEnv.getCatalog("PAIMON").get();
        ObjectPath functionObjectPath = new ObjectPath(DATABASE_NAME, functionName);
        assertThat(batchSql(String.format("SHOW FUNCTIONS in %s", DATABASE_NAME)))
                .contains(Row.of(functionName));
        CatalogFunction getFunction = catalog.getFunction(functionObjectPath);
        assertThat(getFunction.getClassName()).isEqualTo("xxxx");
        result = sql(String.format("CALL sys.drop_function('%s.%s')", DATABASE_NAME, functionName));
        assertThat(result.toString()).contains("Success");
        assertThat(batchSql(String.format("SHOW FUNCTIONS in %s", DATABASE_NAME)))
                .doesNotContain(Row.of(functionName));
    }
}
