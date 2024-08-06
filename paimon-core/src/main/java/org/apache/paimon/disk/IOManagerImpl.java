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

import org.apache.paimon.disk.FileIOChannel.Enumerator;
import org.apache.paimon.disk.FileIOChannel.ID;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/** The facade for the provided I/O manager services. */
public class IOManagerImpl implements IOManager {

    protected static final Logger LOG = LoggerFactory.getLogger(IOManager.class);

    private static final String DIR_NAME_PREFIX = "io";

    private final String[] tempDirs;

    private final FileChannelManager fileChannelManager;

    // -------------------------------------------------------------------------
    //               Constructors / Destructors
    // -------------------------------------------------------------------------

    /**
     * Constructs a new IOManager.
     *
     * @param tempDirs The basic directories for files underlying anonymous channels.
     */
    public IOManagerImpl(String... tempDirs) {
        this.tempDirs = tempDirs;
        this.fileChannelManager =
                new FileChannelManagerImpl(Preconditions.checkNotNull(tempDirs), DIR_NAME_PREFIX);
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Created a new {} for spilling of task related data to disk (joins, sorting, ...). Used directories:\n\t{}",
                    FileChannelManager.class.getSimpleName(),
                    Arrays.stream(fileChannelManager.getPaths())
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining("\n\t")));
        }
    }

    /** Removes all temporary files. */
    @Override
    public void close() throws Exception {
        fileChannelManager.close();
    }

    @Override
    public ID createChannel() {
        return fileChannelManager.createChannel();
    }

    @Override
    public ID createChannel(String prefix) {
        return fileChannelManager.createChannel(prefix);
    }

    @Override
    public String[] tempDirs() {
        return tempDirs;
    }

    @Override
    public Enumerator createChannelEnumerator() {
        return fileChannelManager.createChannelEnumerator();
    }

    /**
     * Deletes the file underlying the given channel. If the channel is still open, this call may
     * fail.
     *
     * @param channel The channel to be deleted.
     */
    public static void deleteChannel(ID channel) {
        if (channel != null) {
            if (channel.getPathFile().exists() && !channel.getPathFile().delete()) {
                LOG.warn("IOManager failed to delete temporary file {}", channel.getPath());
            }
        }
    }

    /**
     * Gets the directories that the I/O manager spills to.
     *
     * @return The directories that the I/O manager spills to.
     */
    public File[] getSpillingDirectories() {
        return fileChannelManager.getPaths();
    }

    /**
     * Gets the directories that the I/O manager spills to, as path strings.
     *
     * @return The directories that the I/O manager spills to, as path strings.
     */
    public String[] getSpillingDirectoriesPaths() {
        File[] paths = fileChannelManager.getPaths();
        String[] strings = new String[paths.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = paths[i].getAbsolutePath();
        }
        return strings;
    }

    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileWriterImpl(channelID);
    }

    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileReaderImpl(channelID);
    }

    public static String[] splitPaths(@Nonnull String separatedPaths) {
        return separatedPaths.length() > 0
                ? separatedPaths.split(",|" + File.pathSeparator)
                : new String[0];
    }
}
