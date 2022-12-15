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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Optional;

/** Table maintenance actions for Flink. */
public class FlinkActions {

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public static CompactAction compact(Path tablePath) {
        return new CompactAction(tablePath);
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printHelp();
            System.exit(1);
        }

        String action = args[0].toLowerCase();
        if ("compact".equals(action)) {
            runCompact(Arrays.copyOfRange(args, 1, args.length));
        } else {
            System.err.println("Unknown action \"" + action + "\"");
            printHelp();
            System.exit(1);
        }
    }

    private static void runCompact(String[] args) throws Exception {
        Optional<CompactAction> action = CompactAction.create(MultipleParameterTool.fromArgs(args));
        if (action.isPresent()) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            action.get().build(env);
            env.execute("Compact job: " + String.join(" ", args));
        } else {
            System.exit(1);
        }
    }

    private static void printHelp() {
        System.out.println("Usage: <action> [OPTIONS]");
        System.out.println();

        System.out.println("Available actions:");
        System.out.println("  compact");
        System.out.println();

        System.out.println("For detailed options of each action, run <action> --help");
    }
}
