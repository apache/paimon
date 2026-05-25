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

/** Renames a table in the catalog. */
public class RenameTableCommand implements Command {

    @Override
    public String name() {
        return "rename-table";
    }

    @Override
    public String description() {
        return "Rename a table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String fromId = parsed.positional(0, "SOURCE_DATABASE.TABLE");
        String toId = parsed.positional(1, "TARGET_DATABASE.TABLE");
        String[] fromParts = SchemaCommand.parseTableIdentifier(fromId);
        String[] toParts = SchemaCommand.parseTableIdentifier(toId);

        ctx.getCatalog()
                .renameTable(
                        Identifier.create(fromParts[0], fromParts[1]),
                        Identifier.create(toParts[0], toParts[1]),
                        false);
        System.out.println("Table '" + fromId + "' renamed to '" + toId + "' successfully.");
    }

    @Override
    public String usage() {
        return "Usage: paimon rename-table SOURCE_DB.TABLE TARGET_DB.TABLE\n\n"
                + "Rename a table in the catalog.";
    }
}
