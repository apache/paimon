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

import org.apache.paimon.annotation.Public;

import java.io.IOException;
import java.util.List;

/**
 * Deletes files in one provider request without falling back to individual deletes.
 *
 * <p>A successful invocation confirms every requested file as deleted or not found. A failure or
 * timeout only means that the complete batch was not confirmed; the provider may already have
 * deleted some files. If a caller retries, it must retry the same complete batch. Implementations
 * must validate the complete request before accessing storage.
 *
 * @since 2.1
 */
@Public
public interface BatchFileDeleter {

    /** Maximum number of files accepted by one {@link #delete(List)} invocation. */
    int maxBatchSize();

    /**
     * Deletes one non-empty batch.
     *
     * @return files confirmed deleted or not found
     * @throws IOException if any requested file cannot be confirmed
     */
    BatchDeleteResult delete(List<Path> files) throws IOException;
}
