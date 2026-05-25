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

import org.apache.paimon.catalog.Database;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

import java.util.Map;

/** Displays database information in JSON format. */
public class GetDatabaseCommand implements Command {

    @Override
    public String name() {
        return "get-database";
    }

    @Override
    public String description() {
        return "Show database information";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed = new CliArgs(args);
        String database = parsed.positional(0, "DATABASE");

        Database db = ctx.getCatalog().getDatabase(database);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"name\":\"").append(escapeJson(db.name())).append("\"");
        sb.append(",\"options\":{");
        Map<String, String> options = db.options();
        if (options != null) {
            int idx = 0;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (idx > 0) {
                    sb.append(",");
                }
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
                sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
                idx++;
            }
        }
        sb.append("}");
        String comment = db.comment().orElse(null);
        if (comment != null) {
            sb.append(",\"comment\":\"").append(escapeJson(comment)).append("\"");
        }
        sb.append("}");
        System.out.println(sb.toString());
    }

    @Override
    public String usage() {
        return "Usage: paimon get-database DATABASE\n\n"
                + "Show database information in JSON format.";
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
