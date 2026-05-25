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

package org.apache.paimon.cli;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lightweight argument parser supporting {@code --key value}, {@code -k value} short aliases, and
 * positional arguments.
 */
public class CliArgs {

    private final Map<String, String> options = new HashMap<>();
    private final String[] positional;

    public CliArgs(String[] args, String... aliases) {
        this(args, Collections.emptySet(), aliases);
    }

    /**
     * @param args raw command-line arguments
     * @param booleanFlags flag names (without dashes) that take no value
     * @param aliases pairs of short-to-long mappings, e.g. "-s", "--select"
     */
    public CliArgs(String[] args, Set<String> booleanFlags, String... aliases) {
        Map<String, String> aliasMap = new HashMap<>();
        for (int i = 0; i + 1 < aliases.length; i += 2) {
            aliasMap.put(aliases[i], aliases[i + 1]);
        }

        List<String> posList = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                String key = aliasMap.getOrDefault(arg, arg);
                key = key.replaceFirst("^--?", "");
                if (booleanFlags.contains(key)) {
                    options.put(key, "true");
                } else if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    options.put(key, args[++i]);
                } else {
                    options.put(key, "true");
                }
            } else {
                posList.add(arg);
            }
        }
        positional = posList.toArray(new String[0]);
    }

    @Nullable
    public String get(String key) {
        return options.get(key);
    }

    public String getOrDefault(String key, String defaultValue) {
        String value = options.get(key);
        return value != null ? value : defaultValue;
    }

    public String require(String key, String description) {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            System.err.println("Missing required option: --" + key + " (" + description + ")");
            System.exit(1);
        }
        return value;
    }

    public String positional(int index, String description) {
        if (index >= positional.length) {
            System.err.println("Missing argument: " + description);
            System.exit(1);
        }
        return positional[index];
    }

    public int positionalCount() {
        return positional.length;
    }

    public boolean hasHelp() {
        return "true".equals(options.get("help")) || "true".equals(options.get("h"));
    }
}
