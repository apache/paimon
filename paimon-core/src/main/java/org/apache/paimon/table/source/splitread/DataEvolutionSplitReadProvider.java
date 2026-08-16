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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

/** A {@link SplitReadProvider} to create {@link DataEvolutionSplitRead}. */
public class DataEvolutionSplitReadProvider implements SplitReadProvider {

    private final LazyField<DataEvolutionSplitRead> splitRead;

    public DataEvolutionSplitReadProvider(
            Supplier<DataEvolutionSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            DataEvolutionSplitRead read = supplier.get();
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    @Override
    public boolean match(Split split, Context context) {
        return true;
    }

    @Override
    public LazyField<DataEvolutionSplitRead> get() {
        return splitRead;
    }
}
