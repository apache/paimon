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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Stream that permits vectored reading.
 */
public interface VectoredReadable {

    /**
     * Read fully a list of file ranges asynchronously from this file.
     *
     * <p> As a result of the call, each range will have FileRange.setData(CompletableFuture)
     * called with a future that when complete will have a ByteBuffer with the
     * data from the file's range.
     */
    void readVectored(List<? extends FileRange> ranges,
                              IntFunction<ByteBuffer> allocate) throws IOException;
}
