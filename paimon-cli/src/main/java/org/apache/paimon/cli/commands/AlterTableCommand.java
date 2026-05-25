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
import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Alters a table's schema or options. */
public class AlterTableCommand implements Command {

    private static final Set<String> BOOLEAN_FLAGS;

    static {
        Set<String> flags = new HashSet<>();
        flags.add("ignore-if-not-exists");
        flags.add("first");
        BOOLEAN_FLAGS = Collections.unmodifiableSet(flags);
    }

    @Override
    public String name() {
        return "alter-table";
    }

    @Override
    public String description() {
        return "Alter table schema or options";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args,
                        BOOLEAN_FLAGS,
                        "-n",
                        "--name",
                        "-t",
                        "--type",
                        "-c",
                        "--comment",
                        "-k",
                        "--key",
                        "-v",
                        "--value",
                        "-m",
                        "--new-name",
                        "--after",
                        "--after",
                        "--first",
                        "--first",
                        "-i",
                        "--ignore-if-not-exists");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String action = parsed.positional(1, "ACTION");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);
        boolean ignoreIfNotExists = "true".equals(parsed.get("ignore-if-not-exists"));

        List<SchemaChange> changes = new ArrayList<>();

        switch (action) {
            case "set-option":
                {
                    String key = parsed.require("key", "option key");
                    String value = parsed.require("value", "option value");
                    changes.add(SchemaChange.setOption(key, value));
                    break;
                }
            case "remove-option":
                {
                    String key = parsed.require("key", "option key");
                    changes.add(SchemaChange.removeOption(key));
                    break;
                }
            case "add-column":
                {
                    String colName = parsed.require("name", "column name");
                    String typeName = parsed.require("type", "column type");
                    String comment = parsed.get("comment");
                    SchemaChange.Move move = parseMove(parsed, colName);
                    changes.add(
                            SchemaChange.addColumn(
                                    colName,
                                    CreateTableCommand.parseDataType(typeName),
                                    comment,
                                    move));
                    break;
                }
            case "drop-column":
                {
                    String colName = parsed.require("name", "column name");
                    changes.add(SchemaChange.dropColumn(colName));
                    break;
                }
            case "rename-column":
                {
                    String colName = parsed.require("name", "current column name");
                    String newName = parsed.require("new-name", "new column name");
                    changes.add(SchemaChange.renameColumn(colName, newName));
                    break;
                }
            case "alter-column":
                {
                    String colName = parsed.require("name", "column name");
                    String typeName = parsed.get("type");
                    String comment = parsed.get("comment");
                    if (typeName != null) {
                        changes.add(
                                SchemaChange.updateColumnType(
                                        colName, CreateTableCommand.parseDataType(typeName)));
                    }
                    if (comment != null) {
                        changes.add(SchemaChange.updateColumnComment(colName, comment));
                    }
                    SchemaChange.Move move = parseMove(parsed, colName);
                    if (move != null) {
                        changes.add(SchemaChange.updateColumnPosition(move));
                    }
                    if (changes.isEmpty()) {
                        System.err.println(
                                "Must specify at least one of --type, --comment, --first, or --after.");
                        return;
                    }
                    break;
                }
            case "update-comment":
                {
                    String comment = parsed.require("comment", "table comment");
                    changes.add(SchemaChange.updateComment(comment));
                    break;
                }
            default:
                System.err.println("Unknown alter action: " + action);
                System.err.println(usage());
                return;
        }

        ctx.getCatalog()
                .alterTable(Identifier.create(parts[0], parts[1]), changes, ignoreIfNotExists);
        System.out.println("Table '" + tableId + "' altered successfully.");
    }

    private static SchemaChange.Move parseMove(CliArgs parsed, String colName) {
        boolean first = "true".equals(parsed.get("first"));
        String after = parsed.get("after");
        if (first) {
            return SchemaChange.Move.first(colName);
        } else if (after != null) {
            return SchemaChange.Move.after(colName, after);
        }
        return null;
    }

    @Override
    public String usage() {
        return "Usage: paimon alter-table DATABASE.TABLE ACTION [options]\n\n"
                + "Actions:\n"
                + "  set-option      Set a table option (--key K --value V)\n"
                + "  remove-option   Remove a table option (--key K)\n"
                + "  add-column      Add a column (--name N --type T [--comment C] "
                + "[--first|--after REF])\n"
                + "  drop-column     Drop a column (--name N)\n"
                + "  rename-column   Rename a column (--name OLD --new-name NEW)\n"
                + "  alter-column    Alter a column (--name N [--type T] [--comment C] "
                + "[--first|--after REF])\n"
                + "  update-comment  Update table comment (--comment C)\n\n"
                + "Options:\n"
                + "  -i, --ignore-if-not-exists   Do not raise error if table does not exist";
    }
}
