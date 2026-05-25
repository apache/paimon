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

/**
 * SPI interface for CLI commands. Implementations are discovered via {@link
 * java.util.ServiceLoader}. External projects can register additional commands by providing a
 * META-INF/services/org.apache.paimon.cli.Command file.
 */
public interface Command {

    /** Command name used on the command line (e.g. "read", "list-tables"). */
    String name();

    /** One-line description shown in help output. */
    String description();

    /**
     * Execute the command.
     *
     * @param ctx provides access to Catalog and Options
     * @param args remaining arguments after the command name
     */
    void execute(CommandContext ctx, String[] args) throws Exception;

    /** Multi-line usage/help text. Printed when --help is passed. */
    default String usage() {
        return "Usage: paimon " + name() + " [options]";
    }
}
