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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.sink.ChannelComputer;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/** Shuffle tables from source identifier and target identifier by target identifier. */
public class ShuffleIdentifierByTableComputer
        implements ChannelComputer<Tuple2<Identifier, Identifier>> {

    private static final long serialVersionUID = 1L;

    private transient int numChannels;

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
    }

    @Override
    public int channel(Tuple2<Identifier, Identifier> record) {
        return Math.floorMod(
                Objects.hash(record.f1.getDatabaseName(), record.f1.getTableName()), numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by identifier hash";
    }
}
