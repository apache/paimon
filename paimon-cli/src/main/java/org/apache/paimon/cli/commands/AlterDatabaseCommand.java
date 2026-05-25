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

import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.cli.CliArgs;
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Alters database properties. */
public class AlterDatabaseCommand implements Command {

    private static final Set<String> BOOLEAN_FLAGS;

    static {
        Set<String> flags = new HashSet<>();
        flags.add("ignore-if-not-exists");
        BOOLEAN_FLAGS = Collections.unmodifiableSet(flags);
    }

    @Override
    public String name() {
        return "alter-database";
    }

    @Override
    public String description() {
        return "Alter database properties";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        CliArgs parsed =
                new CliArgs(
                        args,
                        BOOLEAN_FLAGS,
                        "--set",
                        "--set",
                        "--remove",
                        "--remove",
                        "-i",
                        "--ignore-if-not-exists");
        String database = parsed.positional(0, "DATABASE");
        boolean ignoreIfNotExists = "true".equals(parsed.get("ignore-if-not-exists"));

        String setStr = parsed.get("set");
        String removeStr = parsed.get("remove");

        if (setStr == null && removeStr == null) {
            System.err.println("Must specify --set or --remove.");
            System.err.println(usage());
            return;
        }

        List<PropertyChange> changes = new ArrayList<>();
        if (setStr != null) {
            for (String kv : setStr.split(",")) {
                String[] pair = kv.split("=", 2);
                if (pair.length == 2) {
                    changes.add(PropertyChange.setProperty(pair[0].trim(), pair[1].trim()));
                }
            }
        }
        if (removeStr != null) {
            for (String key : removeStr.split(",")) {
                changes.add(PropertyChange.removeProperty(key.trim()));
            }
        }

        ctx.getCatalog().alterDatabase(database, changes, ignoreIfNotExists);
        System.out.println("Database '" + database + "' altered successfully.");
    }

    @Override
    public String usage() {
        return "Usage: paimon alter-database DATABASE [options]\n\n"
                + "Alter database properties.\n\n"
                + "Options:\n"
                + "  --set key1=value1,key2=value2   Set properties\n"
                + "  --remove key1,key2              Remove properties\n"
                + "  -i, --ignore-if-not-exists      Do not raise error if database does not exist";
    }
}
