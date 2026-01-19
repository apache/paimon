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

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Branch}. */
public interface BranchManager {

    String BRANCH_PREFIX = "branch-";

    void createBranch(String branchName);

    void createBranch(String branchName, @Nullable String tagName);

    void dropBranch(String branchName);

    void fastForward(String branchName);

    List<String> branches();

    /**
     * Get all branches that were created based on the given tag.
     *
     * @param tagName the name of the tag to check
     * @return list of branch names that reference the given tag
     */
    default List<String> branchesCreatedFromTag(String tagName) {
        return java.util.Collections.emptyList();
    }

    default boolean branchExists(String branchName) {
        return branches().contains(branchName);
    }

    /** Return the path string of a branch. */
    static String branchPath(Path tablePath, String branch) {
        return isMainBranch(branch)
                ? tablePath.toString()
                : tablePath.toString() + "/branch/" + BRANCH_PREFIX + branch;
    }

    static String normalizeBranch(String branch) {
        return StringUtils.isNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    static boolean isMainBranch(String branch) {
        return branch.equals(DEFAULT_MAIN_BRANCH);
    }

    static void validateBranch(String branchName) {
        checkArgument(
                !BranchManager.isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);
    }

    static void fastForwardValidate(String branchName, String currentBranch) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                "Branch name '%s' do not use in fast-forward.",
                branchName);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.equals(currentBranch),
                "Fast-forward from the current branch '%s' is not allowed.",
                branchName);
    }
}
