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

package org.apache.paimon.flink.action.cdc.format;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;

/** Factory to create {@link DataFormat}. */
public interface DataFormatFactory extends Factory {

    DataFormat create();

    static DataFormat createDataFormat(String format) {
        String identifier = format.toLowerCase();
        DataFormatFactory dataFormatFactory;
        try {
            dataFormatFactory =
                    FactoryUtil.discoverFactory(
                            DataFormatFactory.class.getClassLoader(),
                            DataFormatFactory.class,
                            identifier);
        } catch (FactoryException e) {
            throw new UnsupportedOperationException(
                    String.format("This format: %s is not supported.", format));
        }
        return dataFormatFactory.create();
    }
}
