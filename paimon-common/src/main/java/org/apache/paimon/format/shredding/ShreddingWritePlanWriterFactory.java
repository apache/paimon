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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;
import java.util.Collections;

/** Decorates a format writer factory with optional per-file shredding write plans. */
public class ShreddingWritePlanWriterFactory
        implements FormatWriterFactory, SupportsShreddingWritePlan {

    private final FormatWriterFactory delegate;
    private final ShreddingWritePlanFactory writePlanFactory;

    public ShreddingWritePlanWriterFactory(
            FormatWriterFactory delegate, ShreddingWritePlanFactory writePlanFactory) {
        this.delegate = delegate;
        this.writePlanFactory = writePlanFactory;
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        if (!writePlanFactory.shouldCreateWritePlan()
                || !(delegate instanceof SupportsShreddingWritePlan)) {
            return delegate.create(out, compression);
        }

        SupportsShreddingWritePlan shreddingDelegate = (SupportsShreddingWritePlan) delegate;
        if (writePlanFactory.shouldInferWritePlan()) {
            return new InferShreddingWritePlanWriter(
                    shreddingDelegate, writePlanFactory, out, compression);
        }

        return createWriterWithPlan(
                shreddingDelegate,
                out,
                compression,
                writePlanFactory.createWritePlan(Collections.emptyList()));
    }

    @Override
    public FormatWriter createWithShreddingWritePlan(
            PositionOutputStream out, String compression, ShreddingWritePlan writePlan)
            throws IOException {
        if (writePlanFactory.shouldCreateWritePlan()) {
            throw new UnsupportedOperationException(
                    "Composing multiple active shredding write plans is not supported.");
        }

        return shreddingDelegate().createWithShreddingWritePlan(out, compression, writePlan);
    }

    @Override
    public void commitShreddingMetadata(
            FormatWriter writer, ShreddingWritePlan writePlan, String compression)
            throws IOException {
        if (writePlanFactory.shouldCreateWritePlan()) {
            throw new UnsupportedOperationException(
                    "Composing multiple active shredding write plans is not supported.");
        }

        shreddingDelegate().commitShreddingMetadata(writer, writePlan, compression);
    }

    private SupportsShreddingWritePlan shreddingDelegate() {
        if (!(delegate instanceof SupportsShreddingWritePlan)) {
            throw new UnsupportedOperationException(
                    "Delegate writer factory does not support shredding write plans: "
                            + delegate.getClass().getName());
        }
        return (SupportsShreddingWritePlan) delegate;
    }

    static FormatWriter createWriterWithPlan(
            SupportsShreddingWritePlan delegate,
            PositionOutputStream out,
            String compression,
            ShreddingWritePlan writePlan)
            throws IOException {
        return new ShreddingFormatWriter(
                delegate.createWithShreddingWritePlan(out, compression, writePlan),
                delegate,
                writePlan,
                compression);
    }
}
