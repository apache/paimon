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
import org.apache.paimon.format.CloseShieldOutputStream;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/** A {@link FormatWriter} implementation for JSON format. */
public class JsonFormatWriter implements FormatWriter {

    private final PositionOutputStream outputStream;
    private final Writer writer;
    private final RowToJsonConverter converter;
    private final ObjectMapper objectMapper;

    public JsonFormatWriter(PositionOutputStream outputStream, RowType rowType, Options options) {
        this.outputStream = outputStream;
        this.writer =
                new BufferedWriter(
                        new OutputStreamWriter(
                                new CloseShieldOutputStream(outputStream), StandardCharsets.UTF_8));
        this.converter = new RowToJsonConverter(rowType, options);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        Object jsonObject = converter.convert(element);
        String jsonString = objectMapper.writeValueAsString(jsonObject);
        writer.write(jsonString);
        writer.write('\n'); // JSON lines format - one JSON object per line
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (outputStream != null && suggestedCheck) {
            writer.flush(); // Ensure all data is written to the stream
            return outputStream.getPos() >= targetSize;
        }
        return false;
    }
}
