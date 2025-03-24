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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link BranchManager} implementation to manage branches via catalog. */
public class CatalogBranchManager implements BranchManager {

    private final CatalogLoader catalogLoader;
    private final Identifier identifier;

    public CatalogBranchManager(CatalogLoader catalogLoader, Identifier identifier) {
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
    }

    private void executePost(ThrowingConsumer<Catalog, Exception> func) {
        executeGet(
                catalog -> {
                    try {
                        func.accept(catalog);
                        return null;
                    } catch (Catalog.BranchNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Branch name '%s' doesn't exist.", e.branch()));
                    } catch (Catalog.TagNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Tag '%s' doesn't exist.", e.tag()));
                    } catch (Catalog.BranchAlreadyExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Branch name '%s' already exists..", e.branch()));
                    }
                });
    }

    private <T> T executeGet(FunctionWithException<Catalog, T, Exception> func) {
        try (Catalog catalog = catalogLoader.load()) {
            return func.apply(catalog);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createBranch(String branchName) {
        executePost(catalog -> catalog.createBranch(identifier, branchName, null));
    }

    @Override
    public void createBranch(String branchName, @Nullable String tagName) {
        executePost(
                catalog -> {
                    BranchManager.validateBranch(branchName);
                    catalog.createBranch(identifier, branchName, tagName);
                });
    }

    @Override
    public void dropBranch(String branchName) {
        executePost(catalog -> catalog.dropBranch(identifier, branchName));
    }

    @Override
    public void fastForward(String branchName) {
        executePost(
                catalog -> {
                    BranchManager.fastForwardValidate(branchName);
                    catalog.fastForward(identifier, branchName);
                });
    }

    @Override
    public List<String> branches() {
        return executeGet(catalog -> catalog.listBranches(identifier));
    }
}
