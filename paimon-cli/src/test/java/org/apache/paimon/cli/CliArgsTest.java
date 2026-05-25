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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CliArgsTest {

    @Test
    void testPositionalArgs() {
        CliArgs args = new CliArgs(new String[] {"mydb.mytable", "extra"});
        assertThat(args.positionalCount()).isEqualTo(2);
        assertThat(args.positional(0, "table")).isEqualTo("mydb.mytable");
        assertThat(args.positional(1, "extra")).isEqualTo("extra");
    }

    @Test
    void testKeyValueOptions() {
        CliArgs args = new CliArgs(new String[] {"--select", "id,name", "--limit", "50"});
        assertThat(args.get("select")).isEqualTo("id,name");
        assertThat(args.get("limit")).isEqualTo("50");
        assertThat(args.positionalCount()).isEqualTo(0);
    }

    @Test
    void testShortAliases() {
        CliArgs args =
                new CliArgs(
                        new String[] {"-s", "id,name", "-l", "10"},
                        "-s",
                        "--select",
                        "-l",
                        "--limit");
        assertThat(args.get("select")).isEqualTo("id,name");
        assertThat(args.get("limit")).isEqualTo("10");
    }

    @Test
    void testBooleanFlags() {
        Set<String> flags = new HashSet<>();
        flags.add("cascade");
        flags.add("ignore-if-exists");
        CliArgs args =
                new CliArgs(
                        new String[] {"mydb", "--cascade", "--ignore-if-exists"},
                        flags,
                        "-i",
                        "--ignore-if-exists");
        assertThat(args.positional(0, "db")).isEqualTo("mydb");
        assertThat(args.get("cascade")).isEqualTo("true");
        assertThat(args.get("ignore-if-exists")).isEqualTo("true");
    }

    @Test
    void testMixedPositionalAndOptions() {
        CliArgs args =
                new CliArgs(
                        new String[] {"mydb.table", "--format", "json", "--limit", "20"},
                        "-f",
                        "--format",
                        "-l",
                        "--limit");
        assertThat(args.positional(0, "table")).isEqualTo("mydb.table");
        assertThat(args.get("format")).isEqualTo("json");
        assertThat(args.get("limit")).isEqualTo("20");
    }

    @Test
    void testHasHelp() {
        CliArgs args1 = new CliArgs(new String[] {"--help"});
        assertThat(args1.hasHelp()).isTrue();

        CliArgs args2 = new CliArgs(new String[] {"-h"});
        assertThat(args2.hasHelp()).isTrue();

        CliArgs args3 = new CliArgs(new String[] {"mydb"});
        assertThat(args3.hasHelp()).isFalse();
    }

    @Test
    void testGetOrDefault() {
        CliArgs args = new CliArgs(new String[] {"--format", "csv"});
        assertThat(args.getOrDefault("format", "tsv")).isEqualTo("csv");
        assertThat(args.getOrDefault("limit", "100")).isEqualTo("100");
    }

    @Test
    void testNullForMissingOption() {
        CliArgs args = new CliArgs(new String[] {"mydb"});
        assertThat(args.get("select")).isNull();
    }
}
