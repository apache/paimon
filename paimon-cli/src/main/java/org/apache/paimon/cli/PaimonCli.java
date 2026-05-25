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

import org.apache.paimon.options.Options;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Main entry point for the Paimon CLI. Discovers {@link Command} implementations via SPI and
 * dispatches to the matching command.
 *
 * <p>Usage: {@code java -jar paimon-cli.jar [--config paimon.yaml] <command> [args...]}
 *
 * <p>Subclasses can override {@link #banner()} and {@link #programName()} to customize the CLI for
 * downstream projects.
 */
public class PaimonCli {

    private final Map<String, Command> commands = new LinkedHashMap<>();

    public PaimonCli() {
        loadCommands(getClass().getClassLoader());
    }

    public PaimonCli(ClassLoader classLoader) {
        loadCommands(classLoader);
    }

    private void loadCommands(ClassLoader classLoader) {
        ServiceLoader<Command> loader = ServiceLoader.load(Command.class, classLoader);
        for (Command cmd : loader) {
            commands.put(cmd.name(), cmd);
        }
    }

    /** Register an additional command programmatically. */
    public void registerCommand(Command command) {
        commands.put(command.name(), command);
    }

    public Map<String, Command> getCommands() {
        return commands;
    }

    protected String programName() {
        return "paimon";
    }

    protected String banner() {
        return "Apache Paimon CLI";
    }

    public void run(String[] args) {
        String configPath = CliConfig.DEFAULT_CONFIG_FILE;
        int argStart = 0;

        // Parse global options before the command name
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (("--config".equals(arg) || "-c".equals(arg)) && i + 1 < args.length) {
                configPath = args[i + 1];
                argStart = i + 2;
                i++;
            } else if ("--help".equals(arg) || "-h".equals(arg)) {
                printUsage();
                return;
            } else if ("--version".equals(arg) || "-v".equals(arg)) {
                printVersion();
                return;
            } else {
                argStart = i;
                break;
            }
        }

        if (argStart >= args.length) {
            printUsage();
            return;
        }

        String commandName = args[argStart];
        String[] subArgs =
                argStart + 1 < args.length
                        ? Arrays.copyOfRange(args, argStart + 1, args.length)
                        : new String[0];

        Command command = commands.get(commandName);
        if (command == null) {
            System.err.println("Unknown command: " + commandName);
            printUsage();
            System.exit(1);
            return;
        }

        // Check for --help on the sub-command
        for (String subArg : subArgs) {
            if ("--help".equals(subArg) || "-h".equals(subArg)) {
                System.err.println(command.usage());
                return;
            }
        }

        Options options = CliConfig.load(configPath);
        try (CommandContext ctx = new CommandContext(options)) {
            command.execute(ctx, subArgs);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private void printUsage() {
        System.err.println(banner());
        System.err.println();
        System.err.println("Usage: " + programName() + " [--config paimon.yaml] <command> [args]");
        System.err.println();
        System.err.println("Commands:");
        for (Command cmd : commands.values()) {
            System.err.println(String.format("  %-20s %s", cmd.name(), cmd.description()));
        }
        System.err.println();
        System.err.println("Global options:");
        System.err.println("  -c, --config PATH   Path to paimon.yaml (default: ./paimon.yaml)");
        System.err.println("  -h, --help          Show this help message");
        System.err.println("  -v, --version       Show version");
    }

    private void printVersion() {
        System.out.println(programName() + " (Apache Paimon CLI)");
    }

    public static void main(String[] args) {
        PaimonCli cli = new PaimonCli();
        cli.run(args);
    }
}
