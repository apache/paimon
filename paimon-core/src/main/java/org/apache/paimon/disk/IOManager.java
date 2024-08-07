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

package org.apache.paimon.disk;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.disk.FileIOChannel.Enumerator;
import org.apache.paimon.disk.FileIOChannel.ID;

import java.io.IOException;

/**
 * The facade for the provided disk I/O services.
 *
 * @since 0.4.0
 */
@Public
public interface IOManager extends AutoCloseable {

    ID createChannel();

    ID createChannel(String prefix);

    String[] tempDirs();

    Enumerator createChannelEnumerator();

    BufferFileWriter createBufferFileWriter(ID channelID) throws IOException;

    BufferFileReader createBufferFileReader(ID channelID) throws IOException;

    static IOManager create(String tempDir) {
        return create(new String[] {tempDir});
    }

    static IOManager create(String[] tempDirs) {
        return new IOManagerImpl(tempDirs);
    }
}
