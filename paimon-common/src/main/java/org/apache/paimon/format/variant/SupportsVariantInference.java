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

package org.apache.paimon.format.variant;

import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/**
 * Interface for FormatWriterFactory implementations that support variant schema inference.
 *
 * <p>Writers implementing this interface can dynamically update their schema based on inferred
 * variant shredding schemas.
 */
public interface SupportsVariantInference {

    /**
     * Create the writer with the inferred shredding schema using the same output stream and
     * compression settings.
     *
     * @param out The output stream to write to
     * @param compression The compression codec
     * @param inferredShreddingSchema The inferred shredding schema for variant fields
     * @return A new FormatWriter configured with the inferred schema
     * @throws IOException If the writer cannot be created
     */
    FormatWriter createWithShreddingSchema(
            PositionOutputStream out, String compression, RowType inferredShreddingSchema)
            throws IOException;
}
