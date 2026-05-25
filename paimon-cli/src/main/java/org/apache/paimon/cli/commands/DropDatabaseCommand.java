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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Drops a database. */
public class DropDatabaseCommand implements Command {

    private static final Set<String> BOOLEAN_FLAGS;

    static {
        Set<String> flags = new HashSet<>();
        flags.add("ignore-if-not-exists");
        flags.add("cascade");
        BOOLEAN_FLAGS = Collections.unmodifiableSet(flags);
    }

    @Override
    public String name() {
        return "drop-database";
    }

    @Override
    public String description() {
        return "Drop a database";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args,
                        BOOLEAN_FLAGS,
                        "-i",
                        "--ignore-if-not-exists",
                        "--cascade",
                        "--cascade");
        String database = parsed.positional(0, "DATABASE");
        boolean ignoreIfNotExists = "true".equals(parsed.get("ignore-if-not-exists"));
        boolean cascade = "true".equals(parsed.get("cascade"));

        ctx.getCatalog().dropDatabase(database, ignoreIfNotExists, cascade);
        System.out.println("Database '" + database + "' dropped successfully.");
    }

    @Override
    public String usage() {
        return "Usage: paimon drop-database DATABASE [options]\n\n"
                + "Drop a database.\n\n"
                + "Options:\n"
                + "  -i, --ignore-if-not-exists   Do not raise error if database does not exist\n"
                + "  --cascade                    Drop all tables before dropping database";
    }
}
