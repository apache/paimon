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

package org.apache.paimon.format;

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/** A factory to create {@link FormatWriter} for file. */
public interface FormatWriterFactory {

    /**
     * Creates a writer that writes to the given stream.
     *
     * @param out The output stream to write the encoded data to.
     * @param compression the compression value.
     * @throws IOException Thrown if the writer cannot be opened, or if the output stream throws an
     *     exception.
     */
    FormatWriter create(PositionOutputStream out, String compression) throws IOException;
}
