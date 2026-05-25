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

package org.apache.paimon.cli.commands;

import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

import java.util.Collections;
import java.util.List;

/** Lists all tables in a database. */
public class ListTablesCommand implements Command {

    @Override
    public String name() {
        return "list-tables";
    }

    @Override
    public String description() {
        return "List all tables in a database";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String database = parsed.positional(0, "DATABASE");

        List<String> tables = ctx.getCatalog().listTables(database);
        if (tables.isEmpty()) {
            System.out.println("No tables found in database '" + database + "'.");
        } else {
            Collections.sort(tables);
            for (String table : tables) {
                System.out.println(table);
            }
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon list-tables DATABASE\n\nList all tables in the specified database.";
    }
}
