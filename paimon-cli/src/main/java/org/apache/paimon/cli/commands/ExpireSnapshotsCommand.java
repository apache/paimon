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
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.Table;

/** Expires old snapshots from a table. */
public class ExpireSnapshotsCommand implements Command {

    @Override
    public String name() {
        return "expire-snapshots";
    }

    @Override
    public String description() {
        return "Expire old snapshots from a table";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args, "-r", "--retain-max", "-m", "--retain-min");
        String tableId = parsed.positional(0, "DATABASE.TABLE");
        String[] parts = SchemaCommand.parseTableIdentifier(tableId);

        String retainMax = parsed.get("retain-max");
        String retainMin = parsed.get("retain-min");

        Table table = ctx.getCatalog().getTable(Identifier.create(parts[0], parts[1]));
        ExpireSnapshots expire = table.newExpireSnapshots();

        ExpireConfig.Builder configBuilder = ExpireConfig.builder();
        if (retainMax != null) {
            configBuilder.snapshotRetainMax(Integer.parseInt(retainMax));
        }
        if (retainMin != null) {
            configBuilder.snapshotRetainMin(Integer.parseInt(retainMin));
        }
        expire.config(configBuilder.build());

        System.err.println("Expiring snapshots for '" + tableId + "'...");
        int expired = expire.expire();
        System.out.println("Expired " + expired + " snapshots from '" + tableId + "'.");
    }

    @Override
    public String usage() {
        return "Usage: paimon expire-snapshots DATABASE.TABLE [options]\n\n"
                + "Expire old snapshots from a table.\n\n"
                + "Options:\n"
                + "  -r, --retain-max N   Maximum number of snapshots to retain\n"
                + "  -m, --retain-min N   Minimum number of snapshots to retain";
    }
}
