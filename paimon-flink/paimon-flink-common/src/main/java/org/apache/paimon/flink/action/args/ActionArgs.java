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
package org.apache.paimon.flink.action.args;

import org.apache.paimon.flink.action.Action;

import com.beust.jcommander.Parameter;

import java.util.List;
import java.util.Optional;

/** ActionArgs, used to create action {@link org.apache.paimon.flink.action.Action} */
public abstract class ActionArgs {
    /** Help parameter */
    @Parameter(
            names = {"-h", "--help"},
            help = true,
            description = "Show the usage message")
    protected boolean help = false;

    protected List<String> unknownOptions;

    public abstract Optional<Action> buildAction();

    public abstract StringBuilder usage();

    public List<String> getUnknownOptions() {
        return unknownOptions;
    }

    public void setUnknownOptions(List<String> unknownOptions) {
        this.unknownOptions = unknownOptions;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }
}
