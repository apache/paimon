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

import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

import java.util.List;

/** Lists all databases in the catalog. */
public class ListDatabasesCommand implements Command {

    @Override
    public String name() {
        return "list-databases";
    }

    @Override
    public String description() {
        return "List all databases in the catalog";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        List<String> databases = ctx.getCatalog().listDatabases();
        if (databases.isEmpty()) {
            System.out.println("No databases found.");
        } else {
            for (String db : databases) {
                System.out.println(db);
            }
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon list-databases\n\nList all databases in the catalog.";
    }
}
