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

package org.apache.paimon.fs;

import org.apache.paimon.options.Options;

/**
 * Optional capability for {@link FileIO} implementations that wrap a Hadoop-compatible filesystem
 * and want to expose their underlying hadoop-style options to native readers/writers (such as Lance
 * or Vortex) hosted in modules that cannot depend on the FileIO implementation directly.
 *
 * <p>Without this interface those modules fall back to reflective {@code Class.forName} + {@code
 * Method.invoke} lookups, which silently break when method signatures drift. Implementing this
 * interface gives them a typed entry point.
 *
 * <p>{@code opType} is one of {@code "read"} / {@code "write"} / {@code "meta"}. Implementations
 * that do not vary options by op type may ignore the argument.
 */
public interface HadoopOptionsProvider {

    Options hadoopOptions(Path path, String opType);
}
