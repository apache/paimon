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

import java.util.Optional;

/** Factory to create {@link DeleteAction}. */
public class DeleteActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "delete";

    private static final String WHERE = "where";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String filter = params.get(WHERE);
        if (filter == null) {
            throw new IllegalArgumentException(
                    "Please specify deletion filter. If you want to delete all records, please use overwrite (see doc).");
        }

        DeleteAction action =
                new DeleteAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        filter,
                        catalogConfigMap(params));

        return Optional.of(action);
    }

    public void printHelp() {
        System.out.println("Action \"delete\" deletes data from a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --where <filter_spec>");
        System.out.println("  delete --path <table_path> --where <filter_spec>");
        System.out.println();

        System.out.println(
                "The '--where <filter_spec>' part is equal to the 'WHERE' clause in SQL DELETE statement. If you want to delete all records, please use overwrite (see doc).");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  delete --path hdfs:///path/to/warehouse/test_db.db/test_table --where 'id > (SELECT count(*) FROM employee)'");
        System.out.println(
                "  It's equal to 'DELETE FROM test_table WHERE id > (SELECT count(*) FROM employee)");
    }
}
