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
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.LocalOrphanFilesClean;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** Cleans orphan files from a table or database. */
public class OrphanCleanCommand implements Command {

    @Override
    public String name() {
        return "orphan-clean";
    }

    @Override
    public String description() {
        return "Clean orphan files from a table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String tableId = parsed.positional(0, "DATABASE.TABLE or DATABASE.*");

        boolean dryRun = "true".equals(parsed.get("dry-run"));
        String olderThanStr = parsed.get("older-than");
        String parallelismStr = parsed.get("parallelism");

        long olderThanMillis = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        if (olderThanStr != null) {
            olderThanMillis = System.currentTimeMillis() - parseDuration(olderThanStr);
        }

        Integer parallelism = parallelismStr != null ? Integer.parseInt(parallelismStr) : null;

        String database;
        String tableName;
        if (tableId.contains(".")) {
            String[] parts = tableId.split("\\.", 2);
            database = parts[0];
            tableName = parts[1];
        } else {
            database = tableId;
            tableName = "*";
        }

        List<LocalOrphanFilesClean> cleans =
                LocalOrphanFilesClean.createOrphanFilesCleans(
                        ctx.getCatalog(),
                        database,
                        tableName,
                        olderThanMillis,
                        parallelism,
                        dryRun);

        long totalFiles = 0;
        long totalBytes = 0;

        for (LocalOrphanFilesClean clean : cleans) {
            CleanOrphanFilesResult result = clean.clean();
            totalFiles += result.getDeletedFileCount();
            totalBytes += result.getDeletedFileTotalLenInBytes();
        }

        if (dryRun) {
            System.out.println(
                    "Dry run: would delete "
                            + totalFiles
                            + " orphan files ("
                            + formatBytes(totalBytes)
                            + ").");
        } else {
            System.out.println(
                    "Deleted " + totalFiles + " orphan files (" + formatBytes(totalBytes) + ").");
        }
    }

    private static long parseDuration(String value) {
        String trimmed = value.trim().toLowerCase();
        if (trimmed.endsWith("d")) {
            return TimeUnit.DAYS.toMillis(Long.parseLong(trimmed.replace("d", "")));
        } else if (trimmed.endsWith("h")) {
            return TimeUnit.HOURS.toMillis(Long.parseLong(trimmed.replace("h", "")));
        } else {
            return TimeUnit.DAYS.toMillis(Long.parseLong(trimmed));
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KiB", bytes / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MiB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GiB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    @Override
    public String usage() {
        return "Usage: paimon orphan-clean DATABASE.TABLE [options]\n\n"
                + "Clean orphan files that are not referenced by any snapshot.\n\n"
                + "Options:\n"
                + "  --older-than DURATION  Only delete files older than this (default: 1d)\n"
                + "                         Format: Nd (days) or Nh (hours)\n"
                + "  --dry-run              Show what would be deleted without deleting\n"
                + "  --parallelism N        Thread pool size for deletion\n\n"
                + "Examples:\n"
                + "  paimon orphan-clean mydb.users\n"
                + "  paimon orphan-clean mydb.users --older-than 7d --dry-run\n"
                + "  paimon orphan-clean mydb.* --older-than 3d";
    }
}
