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

package dev.vortex.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A random-access byte source that native code reads from.
 *
 * <p>Native worker threads can call {@link #readFully} concurrently, so implementations must be
 * safe for concurrent positional reads. Native code never calls {@link #close()}. The creator
 * closes it after every data source and scan built on it is closed.
 */
public interface NativeReadable extends Closeable {

    /**
     * A stable, unique name of the source, normally its location. It keys the session footer cache
     * and must not contain glob characters ({@code *?[}).
     */
    String name();

    /** Total length of the source in bytes. */
    long length();

    /**
     * Reads exactly {@code buffer.remaining()} bytes from absolute {@code position}. Short reads
     * are not permitted. {@code buffer} is a direct buffer over native memory, valid only during
     * the call.
     */
    void readFully(long position, ByteBuffer buffer) throws IOException;
}
