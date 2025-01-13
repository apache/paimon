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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link StartingScanner} for incremental changes by tag. */
public class IncrementalTagStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalTagStartingScanner.class);

    private final Snapshot start;
    private final Snapshot end;

    public IncrementalTagStartingScanner(
            SnapshotManager snapshotManager, Snapshot start, Snapshot end) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        this.startingSnapshotId = start.id();
    }

    @Override
    public Result scan(SnapshotReader reader) {
        return StartingScanner.fromPlan(reader.withSnapshot(end).readIncrementalDiff(start));
    }

    public static AbstractStartingScanner create(
            SnapshotManager snapshotManager, String endTagName, CoreOptions options) {
        TagPeriodHandler periodHandler = TagPeriodHandler.create(options);
        checkArgument(
                periodHandler.isAutoTag(endTagName),
                "Specified tag '%s' is not an auto-created tag.",
                endTagName);

        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());

        Optional<Tag> endTag = tagManager.get(endTagName);
        if (!endTag.isPresent()) {
            LOG.info("Tag {} doesn't exist.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        Snapshot end = endTag.get().trimToSnapshot();

        LocalDateTime endTagTime = periodHandler.tagToTime(endTagName);

        List<Pair<Tag, LocalDateTime>> previousTags =
                tagManager.tagObjects().stream()
                        .filter(p -> periodHandler.isAutoTag(p.getRight()))
                        .map(p -> Pair.of(p.getLeft(), periodHandler.tagToTime(p.getRight())))
                        .filter(p -> p.getRight().isBefore(endTagTime))
                        .sorted((tag1, tag2) -> tag2.getRight().compareTo(tag1.getRight()))
                        .collect(Collectors.toList());

        if (previousTags.isEmpty()) {
            LOG.info("Didn't found earlier tags for {}.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        LOG.info("Found start tag {} .", periodHandler.timeToTag(previousTags.get(0).getRight()));
        Snapshot start = previousTags.get(0).getLeft().trimToSnapshot();

        return new IncrementalTagStartingScanner(snapshotManager, start, end);
    }
}
