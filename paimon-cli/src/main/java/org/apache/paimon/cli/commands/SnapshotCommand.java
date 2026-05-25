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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.table.Table;

import java.util.Optional;

/** Displays the latest snapshot information of a table. */
public class SnapshotCommand implements Command {

    @Override
    public String name() {
        return "snapshot";
    }

    @Override
    public String description() {
        return "Show the latest snapshot of a table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        Optional<Snapshot> snapshot = table.latestSnapshot();
        if (snapshot.isPresent()) {
            System.out.println(snapshot.get().toJson());
        } else {
            System.out.println("No snapshot found for '" + tableId + "'.");
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon snapshot DATABASE.TABLE\n\n"
                + "Show the latest snapshot information of a table in JSON format.";
    }
}
