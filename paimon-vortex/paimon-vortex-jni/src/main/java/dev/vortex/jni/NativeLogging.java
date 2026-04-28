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

package dev.vortex.jni;

/**
 * Utility class for configuring native logging levels in the Vortex JNI layer.
 * <p>
 * This class provides constants for different logging levels and methods to
 * initialize native logging to the desired verbosity level. The logging levels
 * correspond to standard logging frameworks with ERROR being the least verbose
 * and TRACE being the most verbose.
 * </p>
 */
public final class NativeLogging {
    static {
        NativeLoader.loadJni();
    }

    private NativeLogging() {}

    /** Logging level constant for error messages only */
    public static final int ERROR = 0;

    /** Logging level constant for warning and error messages */
    public static final int WARN = 1;

    /** Logging level constant for informational, warning, and error messages */
    public static final int INFO = 2;

    /** Logging level constant for debug, informational, warning, and error messages */
    public static final int DEBUG = 3;

    /** Logging level constant for all messages including trace-level debugging */
    public static final int TRACE = 4;

    /**
     * Initialize logging to the desired level. Must be one of:
     * <ul>
     *  <li>{@link #ERROR}</li>
     *  <li>{@link #WARN}</li>
     *  <li>{@link #INFO}</li>
     *  <li>{@link #DEBUG}</li>
     *  <li>{@link #TRACE}</li>
     * </ul>
     */
    public static native void initLogging(int level);
}
