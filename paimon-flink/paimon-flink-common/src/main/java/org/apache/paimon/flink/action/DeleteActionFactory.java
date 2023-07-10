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

package org.apache.paimon.flink.action;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.Action.getTablePath;
import static org.apache.paimon.flink.action.Action.optionalConfigMap;

/** Factory to create {@link DeleteAction}. */
public class DeleteActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "delete";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);

        String filter = params.get("where");
        if (filter == null) {
            throw new IllegalArgumentException(
                    "Please specify deletion filter. If you want to delete all records, please use overwrite (see doc).");
        }

        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        DeleteAction action =
                new DeleteAction(tablePath.f0, tablePath.f1, tablePath.f2, filter, catalogConfig);

        return Optional.of(action);
    }

    public void printHelp() {
        System.out.println("Action \"delete\" deletes data from a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --where <filter_spec>");
        System.out.println("  delete --path <table-path> --where <filter_spec>");
        System.out.println();

        System.out.println(
                "The '--where <filter_spec>' part is equal to the 'WHERE' clause in SQL DELETE statement. If you want to delete all records, please use overwrite (see doc).");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  delete --path hdfs:///path/to/warehouse/test_db.db/test_table --where id > (SELECT count(*) FROM employee)");
        System.out.println(
                "  It's equal to 'DELETE FROM test_table WHERE id > (SELECT count(*) FROM employee)");
    }
}
