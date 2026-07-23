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

package org.apache.paimon.spark.function;

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.ExceptionUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/** Executor-side descriptor-to-presigned-URL function bound to one table's FileIO. */
public class ResolvedDescriptorToPresignedUrlFunction
        implements ScalarFunction<UTF8String>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final long MICROS_PER_SECOND = 1_000_000L;

    private final FileIO fileIO;
    private final Path tableRoot;
    private final boolean ignoreErrors;

    public ResolvedDescriptorToPresignedUrlFunction(
            FileIO fileIO, Path tableRoot, boolean ignoreErrors) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    public DataType[] inputTypes() {
        return new DataType[] {
            DataTypes.BinaryType, DataTypes.StringType, DataTypes.createDayTimeIntervalType()
        };
    }

    @Override
    public DataType resultType() {
        return DataTypes.StringType;
    }

    public UTF8String invoke(byte[] descriptor, UTF8String extension, long validityMicros)
            throws IOException {
        if (descriptor == null || extension == null) {
            return null;
        }

        try {
            Duration validity = validity(validityMicros);
            BlobDescriptor blobDescriptor = BlobDescriptor.deserialize(descriptor);
            return UTF8String.fromString(
                    fileIO.createBlobPresignedUrl(
                            tableRoot, blobDescriptor, extension.toString(), validity));
        } catch (Exception e) {
            if (ignoreErrors) {
                return null;
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public UTF8String produceResult(InternalRow input) {
        if (input.isNullAt(0) || input.isNullAt(1) || input.isNullAt(2)) {
            return null;
        }
        try {
            return invoke(input.getBinary(0), input.getUTF8String(1), input.getLong(2));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }

    private Duration validity(long validityMicros) {
        if (validityMicros <= 0) {
            throw new IllegalArgumentException("Validity interval must be positive.");
        }
        if (validityMicros % MICROS_PER_SECOND != 0) {
            throw new IllegalArgumentException("Validity interval must contain whole seconds.");
        }
        return Duration.ofSeconds(validityMicros / MICROS_PER_SECOND);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public String name() {
        return DescriptorToPresignedUrlUnbound.functionName(ignoreErrors);
    }
}
