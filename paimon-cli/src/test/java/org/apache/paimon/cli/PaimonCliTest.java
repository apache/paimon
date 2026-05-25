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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PaimonCliTest {

    @Test
    void testSpiLoadsAllBuiltinCommands() {
        PaimonCli cli = new PaimonCli();
        Map<String, Command> commands = cli.getCommands();

        assertThat(commands).containsKey("list-databases");
        assertThat(commands).containsKey("list-tables");
        assertThat(commands).containsKey("schema");
        assertThat(commands).containsKey("read");
        assertThat(commands).containsKey("write");
        assertThat(commands).containsKey("create-database");
        assertThat(commands).containsKey("drop-database");
        assertThat(commands).containsKey("create-table");
        assertThat(commands).containsKey("drop-table");
        assertThat(commands).containsKey("orphan-clean");
        assertThat(commands).containsKey("branch");
    }

    @Test
    void testRegisterCustomCommand() {
        PaimonCli cli = new PaimonCli();
        Command custom =
                new Command() {
                    @Override
                    public String name() {
                        return "custom";
                    }

                    @Override
                    public String description() {
                        return "A custom command";
                    }

                    @Override
                    public void execute(CommandContext ctx, String[] args) {}
                };
        cli.registerCommand(custom);
        assertThat(cli.getCommands()).containsKey("custom");
        assertThat(cli.getCommands().get("custom").description()).isEqualTo("A custom command");
    }

    @Test
    void testCommandDescriptionsNotEmpty() {
        PaimonCli cli = new PaimonCli();
        for (Command cmd : cli.getCommands().values()) {
            assertThat(cmd.name()).isNotEmpty();
            assertThat(cmd.description()).isNotEmpty();
            assertThat(cmd.usage()).isNotEmpty();
        }
    }
}
