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
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * A decorator factory that adds variant schema inference capability to any {@link
 * FormatWriterFactory}.
 *
 * <p>This factory wraps an existing FormatWriterFactory and automatically enables variant schema
 * inference if the delegate factory supports it (implements {@link SupportsVariantInference}) and
 * the configuration enables inference.
 */
public class VariantInferenceWriterFactory implements FormatWriterFactory {

    private final FormatWriterFactory delegate;
    private final VariantInferenceConfig config;

    public VariantInferenceWriterFactory(
            FormatWriterFactory delegate, VariantInferenceConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        if (!config.shouldEnableInference()) {
            return delegate.create(out, compression);
        }

        if (!(delegate instanceof SupportsVariantInference)) {
            return delegate.create(out, compression);
        }

        return new InferVariantShreddingWriter(
                (SupportsVariantInference) delegate,
                config.createInferrer(),
                config.getMaxBufferRow(),
                out,
                compression);
    }
}
