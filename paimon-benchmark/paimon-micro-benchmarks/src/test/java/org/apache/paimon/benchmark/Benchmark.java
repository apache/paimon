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

package org.apache.paimon.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/** Utility class to benchmark. */
public class Benchmark {

    private final String name;

    private final long valuesPerIteration;

    private int numWarmupIters;

    private boolean outputPerIteration;

    private final List<Case> cases;

    public Benchmark(String name, long valuesPerIteration) {
        this.name = name;
        this.valuesPerIteration = valuesPerIteration;
        this.numWarmupIters = 1;
        this.outputPerIteration = false;
        this.cases = new ArrayList<>();
    }

    public void addCase(String name, int numIters, Runnable run) {
        this.cases.add(new Case(name, numIters, run));
    }

    public void run() {
        if (cases.isEmpty()) {
            throw new IllegalStateException("No cases.");
        }
        System.out.println("Running benchmark: " + name);

        List<Result> results = new ArrayList<>();
        for (Case c : cases) {
            System.out.println("  Running case: " + c.name);
            results.add(measure(c));
        }

        double firstBest = results.get(0).bestNs;

        System.out.println();
        System.out.println(getJVMOSInfo());
        System.out.println(getProcessorName());
        System.out.printf(
                "%-100s %16s %16s %16s %10s%n",
                name + ":", "Best/Avg Time(ms)", "Row Rate(K/s)", "Per Row(ns)", "Relative");
        System.out.println(
                "----------------------------------------------------"
                        + "-----------------------------------------------------------------------"
                        + "-----------------------------------------");
        for (int i = 0; i < results.size(); ++i) {
            final Case c = cases.get(i);
            final Result r = results.get(i);
            System.out.printf(
                    "%-100s %16s %16s %16s %10s%n",
                    "OPERATORTEST_" + name + "_" + c.name,
                    String.format("%5.0f / %4.0f", r.bestNs / 1000_000.0, r.avgNs / 1000_000.0),
                    String.format("%10.1f", r.bestRate),
                    String.format("%6.1f", 1000000 / r.bestRate),
                    String.format("%3.1fX", (firstBest / r.bestNs)));
        }
        System.out.println("\n\n\n");
    }

    private Result measure(final Case c) {
        System.gc(); // ensures garbage from previous cases don't impact this one

        for (int iter = 0; iter < numWarmupIters; ++iter) {
            c.runnable.run();
        }

        long totalTime = 0;
        long best = Long.MAX_VALUE;
        for (int iter = 0; iter < c.numIters; ++iter) {
            Timer timer = new Timer();
            timer.startTimer();
            c.runnable.run();
            timer.stopTimer();
            long runTime = timer.totalTime();

            totalTime += runTime;
            if (runTime < best) {
                best = runTime;
            }

            if (outputPerIteration) {
                System.out.println(
                        "Iteration " + iter + " took " + runTime / 1000 + " microseconds");
            }
        }
        System.out.println(
                "  Stopped after " + c.numIters + " iterations, " + totalTime / 1000000 + " ms");
        return new Result(
                1.0 * totalTime / c.numIters, valuesPerIteration / (best / 1000000.0), best);
    }

    public Benchmark setOutputPerIteration(boolean outputPerIteration) {
        this.outputPerIteration = outputPerIteration;
        return this;
    }

    public Benchmark setNumWarmupIters(int numWarmupIters) {
        this.numWarmupIters = numWarmupIters;
        return this;
    }

    private String executeAndGetOutput(String[] commands) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(commands).directory(new File("."));
        Process start = builder.start();
        StringBuilder output = new StringBuilder();
        Thread t =
                new Thread(
                        () -> {
                            BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(start.getInputStream()));
                            String s;
                            try {
                                while ((s = reader.readLine()) != null) {
                                    output.append(s).append("\n");
                                }
                            } catch (IOException ignored) {
                            }
                        });
        t.setDaemon(true);
        t.start();
        start.waitFor();
        t.join();
        return output.toString();
    }

    private String getProcessorName() {
        if (SystemUtils.IS_OS_MAC_OSX) {
            try {
                return StringUtils.stripEnd(
                        executeAndGetOutput(
                                new String[] {
                                    "/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"
                                }),
                        null);
            } catch (Throwable e) {
                return "Unknown processor";
            }
        } else if (SystemUtils.IS_OS_LINUX) {
            try {
                String grepPath =
                        StringUtils.stripEnd(
                                executeAndGetOutput(new String[] {"which", "grep"}), null);
                return StringUtils.stripEnd(
                        executeAndGetOutput(
                                new String[] {grepPath, "-m", "1", "model name", "/proc/cpuinfo"}),
                        null);
            } catch (Throwable t) {
                return "Unknown processor";
            }
        } else {
            return System.getenv("PROCESSOR_IDENTIFIER");
        }
    }

    private String getJVMOSInfo() {
        String vmName = System.getProperty("java.vm.name");
        String runtimeVersion = System.getProperty("java.runtime.version");
        String osName = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        return String.format("%s %s on %s %s", vmName, runtimeVersion, osName, osVersion);
    }

    private static class Case {
        private final String name;
        private final int numIters;
        private final Runnable runnable;

        Case(String name, int numIters, Runnable runnable) {
            this.name = name;
            this.numIters = numIters;
            this.runnable = runnable;
        }
    }

    private static class Result {
        private final double avgNs;
        private final double bestRate;
        private final double bestNs;

        Result(double avgNs, double bestRate, double bestNs) {
            this.avgNs = avgNs;
            this.bestRate = bestRate;
            this.bestNs = bestNs;
        }
    }

    private static class Timer {
        private long startTime = 0L;
        private long accumulatedTime = 0L;

        void startTimer() {
            startTime = System.nanoTime();
        }

        void stopTimer() {
            accumulatedTime += System.nanoTime() - startTime;
            startTime = 0L;
        }

        long totalTime() {
            return accumulatedTime;
        }
    }
}
