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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.table.Table;

/** Rolls back a table to a specific snapshot or tag. */
public class RollbackCommand implements Command {

    @Override
    public String name() {
        return "rollback";
    }

    @Override
    public String description() {
        return "Rollback a table to a snapshot or tag";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-s", "--snapshot", "-t", "--tag");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String snapshotId = parsed.get("snapshot");
        String tagName = parsed.get("tag");

        if (snapshotId == null && tagName == null) {
            System.err.println("Must specify --snapshot ID or --tag NAME.");
            System.err.println(usage());
            return;
        }

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));

        if (snapshotId != null) {
            long sid = Long.parseLong(snapshotId);
            table.rollbackTo(sid);
            System.out.println("Table '" + tableId + "' rolled back to snapshot " + sid + ".");
        } else {
            table.rollbackTo(tagName);
            System.out.println("Table '" + tableId + "' rolled back to tag '" + tagName + "'.");
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon rollback DATABASE.TABLE [options]\n\n"
                + "Rollback a table to a specific snapshot or tag.\n\n"
                + "Options:\n"
                + "  -s, --snapshot ID    Rollback to snapshot ID\n"
                + "  -t, --tag NAME       Rollback to tag name";
    }
}
