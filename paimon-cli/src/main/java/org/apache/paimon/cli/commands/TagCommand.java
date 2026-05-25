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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.List;

/** Manages tags on a table (create, delete, list). */
public class TagCommand implements Command {

    @Override
    public String name() {
        return "tag";
    }

    @Override
    public String description() {
        return "Manage table tags (create, delete, list)";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-s", "--snapshot");
        String action = parsed.positional(0, "ACTION (create|delete|list)");
        String tableId = parsed.positional(1, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));

        switch (action) {
            case "create":
                {
                    String tagName = parsed.require("tag-name", "tag name");
                    String snapshotId = parsed.get("snapshot");
                    if (snapshotId != null) {
                        table.createTag(tagName, Long.parseLong(snapshotId));
                    } else {
                        table.createTag(tagName);
                    }
                    System.out.println("Tag '" + tagName + "' created on '" + tableId + "'.");
                    break;
                }
            case "delete":
                {
                    String tagName = parsed.require("tag-name", "tag name");
                    table.deleteTag(tagName);
                    System.out.println("Tag '" + tagName + "' deleted from '" + tableId + "'.");
                    break;
                }
            case "list":
                {
                    if (!(table instanceof FileStoreTable)) {
                        System.err.println("Table does not support tag listing.");
                        return;
                    }
                    List<String> tags = ((FileStoreTable) table).tagManager().allTagNames();
                    if (tags.isEmpty()) {
                        System.out.println("No tags found for '" + tableId + "'.");
                    } else {
                        for (String tag : tags) {
                            System.out.println(tag);
                        }
                    }
                    break;
                }
            default:
                System.err.println("Unknown tag action: " + action);
                System.err.println(usage());
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon tag <action> DATABASE.TABLE [options]\n\n"
                + "Actions:\n"
                + "  create   Create a tag (--tag-name NAME [--snapshot ID])\n"
                + "  delete   Delete a tag (--tag-name NAME)\n"
                + "  list     List all tags\n\n"
                + "Options:\n"
                + "  --tag-name NAME      Tag name (required for create/delete)\n"
                + "  -s, --snapshot ID    Snapshot ID (for create, defaults to latest)";
    }
}
