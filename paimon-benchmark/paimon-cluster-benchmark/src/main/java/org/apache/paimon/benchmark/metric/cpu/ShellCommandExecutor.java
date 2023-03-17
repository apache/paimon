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

package org.apache.paimon.benchmark.metric.cpu;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/** The shell command executor. */
public class ShellCommandExecutor {

    private final String[] command;

    private final StringBuilder output = new StringBuilder();

    private int exitCode;

    /**
     * Instantiates a new Shell command executor.
     *
     * @param execString the exec string
     */
    public ShellCommandExecutor(String[] execString) {
        this.command = execString;
    }

    /**
     * Execute.
     *
     * @throws IOException the io exception
     */
    public void execute() throws IOException, InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder(command);
        final Process process = builder.start();
        exitCode = process.waitFor();
        try (BufferedReader inReader =
                new BufferedReader(
                        new InputStreamReader(
                                process.getInputStream(), Charset.defaultCharset()))) {
            char[] buf = new char[512];
            int readCount;
            while ((readCount = inReader.read(buf, 0, buf.length)) > 0) {
                output.append(buf, 0, readCount);
            }
        }
    }

    /**
     * Gets exit code.
     *
     * @return the exit code
     */
    public int getExitCode() {
        return exitCode;
    }

    /**
     * Gets stdout.
     *
     * @return the output
     */
    public String getOutput() {
        return output.toString();
    }
}
