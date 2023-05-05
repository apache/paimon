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

package org.apache.paimon.utils;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

/** Fail the constructor of input or output stream. */
public class FailingConstructInputOutputIO extends TraceableFileIO {

    private final double rate;
    private final Random random = new Random();
    private String targetName;
    private final HashSet<Path> openedInputPath = new HashSet<>();
    private final HashSet<Path> openedOutputPath = new HashSet<>();

    public FailingConstructInputOutputIO(double rate, Class c) {
        this(rate, c, (String) null);
    }

    public FailingConstructInputOutputIO(double rate, Class c, @Nullable String method) {
        this.rate = rate;
        targetName = "." + c.getSimpleName() + ".";

        if (method != null) {
            targetName += method;
        }
    }

    @Override
    public SeekableInputStream newInputStream(Path f) throws IOException {
        openedInputPath.add(f);
        throwError();
        return super.newInputStream(f);
    }

    @Override
    public PositionOutputStream newOutputStream(Path f, boolean overwrite) throws IOException {
        openedOutputPath.add(f);
        throwError();
        return super.newOutputStream(f, overwrite);
    }

    private void throwError() throws IOException {
        if (ThreadUtils.stackContains(targetName)) {
            if (Math.abs(random.nextInt(100)) < 100 * rate) {
                throw new IOException("emulate real io exception");
            }
        }
    }

    public boolean noLeak() {
        return openInputStreams(s -> openedOutputPath.contains(s)).size() == 0
                && openOutputStreams(s -> openedOutputPath.contains(s)).size() == 0;
    }
}
