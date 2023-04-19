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
package org.apache.paimon.flink.utils;

import org.apache.paimon.flink.action.args.ActionArgs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;

public class ActionLineUtils {
    private ActionLineUtils() {
        throw new UnsupportedOperationException(
                "ActionLineUtils is a utility class and cannot be instantiated");
    }

    public static <T extends ActionArgs> T parse(String[] args, T obj) {
        return parse(args, obj, null, false);
    }

    public static <T extends ActionArgs> T parse(
            String[] args, T obj, String programName, boolean acceptUnknownOptions) {
        JCommander jCommander =
                JCommander.newBuilder()
                        .programName(programName)
                        .addObject(obj)
                        .acceptUnknownOptions(acceptUnknownOptions)
                        .build();
        try {
            jCommander.parse(args);
            // custom parameters
            obj.setUnknownOptions(jCommander.getUnknownOptions());
        } catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            exit(jCommander, programName, obj);
        }

        if (obj.isHelp()) {
            exit(jCommander, programName, obj);
        }
        return obj;
    }

    private static void exit(JCommander jCommander, String programName, ActionArgs args) {
        new UnixStyleUsageFormatter(jCommander).usage(programName, args.usage());
        System.exit(1);
    }
}
