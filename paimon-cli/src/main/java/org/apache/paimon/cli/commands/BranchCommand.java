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
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import java.util.List;

/** Manages branches on a table (create, delete, rename, list, fast-forward, merge). */
public class BranchCommand implements Command {

    @Override
    public String name() {
        return "branch";
    }

    @Override
    public String description() {
        return "Manage table branches";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String action = parsed.positional(0, "ACTION");
        String tableId = parsed.positional(1, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);
        Identifier identifier = Identifier.create(parts[0], parts[1]);

        switch (action) {
            case "create":
                executeCreate(ctx, identifier, parsed);
                break;
            case "delete":
                executeDelete(ctx, identifier, parsed);
                break;
            case "rename":
                executeRename(ctx, identifier, parsed);
                break;
            case "list":
                executeList(ctx, identifier);
                break;
            case "fast-forward":
                executeFastForward(ctx, identifier, parsed);
                break;
            case "merge":
                executeMerge(ctx, identifier, parsed);
                break;
            default:
                System.err.println("Unknown action: " + action);
                System.err.println(
                        "Valid actions: create, delete, rename, list, " + "fast-forward, merge");
        }
    }

    private void executeCreate(CommandContext ctx, Identifier id, CliArgs parsed) throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        String branchName = parsed.require("name", "branch name");
        String tag = parsed.get("tag");
        if (tag != null) {
            table.createBranch(branchName, tag);
            System.out.println(
                    "Branch '" + branchName + "' created from tag '" + tag + "' on '" + id + "'.");
        } else {
            table.createBranch(branchName);
            System.out.println("Branch '" + branchName + "' created on '" + id + "'.");
        }
    }

    private void executeDelete(CommandContext ctx, Identifier id, CliArgs parsed) throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        String branchName = parsed.require("name", "branch name");
        table.deleteBranch(branchName);
        System.out.println("Branch '" + branchName + "' deleted from '" + id + "'.");
    }

    private void executeRename(CommandContext ctx, Identifier id, CliArgs parsed) throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        String branchName = parsed.require("name", "branch name");
        String newName = parsed.require("new-name", "new branch name");
        table.renameBranch(branchName, newName);
        System.out.println(
                "Branch '" + branchName + "' renamed to '" + newName + "' on '" + id + "'.");
    }

    private void executeList(CommandContext ctx, Identifier id) throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        List<String> branches = ((DataTable) table).branchManager().branches();
        if (branches.isEmpty()) {
            System.out.println("No branches found for '" + id + "'.");
        } else {
            for (String branch : branches) {
                System.out.println(branch);
            }
        }
    }

    private void executeFastForward(CommandContext ctx, Identifier id, CliArgs parsed)
            throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        String branchName = parsed.require("name", "branch name");
        table.fastForward(branchName);
        System.out.println("Branch '" + branchName + "' fast-forwarded to main on '" + id + "'.");
    }

    private void executeMerge(CommandContext ctx, Identifier id, CliArgs parsed) throws Exception {
        Table table = ctx.getCatalog().getTable(id);
        String source = parsed.require("source", "source branch");
        String target = parsed.get("target");
        if (target == null) {
            target = "main";
        }
        table.mergeBranch(source, target);
        System.out.println("Branch '" + source + "' merged into '" + target + "' on '" + id + "'.");
    }

    @Override
    public String usage() {
        return "Usage: paimon branch <action> DATABASE.TABLE [options]\n\n"
                + "Actions:\n"
                + "  create         Create a branch (--name NAME [--tag TAG])\n"
                + "  delete         Delete a branch (--name NAME)\n"
                + "  rename         Rename a branch (--name NAME --new-name NEW)\n"
                + "  list           List all branches\n"
                + "  fast-forward   Fast-forward branch to main (--name NAME)\n"
                + "  merge          Merge branches (--source SRC [--target TGT])\n\n"
                + "Options:\n"
                + "  --name NAME       Branch name\n"
                + "  --tag TAG         Create branch from this tag\n"
                + "  --new-name NEW    New name for rename\n"
                + "  --source SRC      Source branch for merge\n"
                + "  --target TGT      Target branch for merge (default: main)";
    }
}
