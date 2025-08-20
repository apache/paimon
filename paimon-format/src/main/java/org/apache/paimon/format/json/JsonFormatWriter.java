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

package org.apache.paimon.format.json;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/** Json format writer implementation. */
public class JsonFormatWriter implements FormatWriter {

    private static final char LINE_SEPARATOR = '\n';

    private final PositionOutputStream outputStream;
    private final BufferedWriter writer;
    private final RowType rowType;

    public JsonFormatWriter(PositionOutputStream outputStream, RowType rowType) {
        this.outputStream = outputStream;
        this.writer =
                new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        this.rowType = rowType;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        String jsonString = JsonSerde.convertRowToJsonString(element, rowType);
        writer.write(jsonString);
        writer.write(LINE_SEPARATOR);
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (suggestedCheck) {
            return outputStream.getPos() >= targetSize;
        }
        return false;
    }
}
