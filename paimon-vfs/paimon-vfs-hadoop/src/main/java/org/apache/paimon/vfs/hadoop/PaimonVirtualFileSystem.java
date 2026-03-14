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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.vfs.VFSCatalogIdentifier;
import org.apache.paimon.vfs.VFSDatabaseIdentifier;
import org.apache.paimon.vfs.VFSIdentifier;
import org.apache.paimon.vfs.VFSOperations;
import org.apache.paimon.vfs.VFSTableIdentifier;
import org.apache.paimon.vfs.VFSTableInfo;
import org.apache.paimon.vfs.VFSTableObjectIdentifier;
import org.apache.paimon.vfs.VFSTableRootIdentifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/** Paimon virtual file system. */
public class PaimonVirtualFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonVirtualFileSystem.class);

    private Path workingDirectory;
    private URI uri;
    private VFSOperations vfsOperations;
    private Configuration conf;

    private static final String USER_AGENT = "HadoopPVFS";
    private static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024L;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        setConf(conf);
        this.conf = conf;
        super.initialize(uri, conf);

        this.workingDirectory = new Path(uri);
        if (uri.getAuthority() == null || uri.getAuthority().isEmpty()) {
            throw new IllegalArgumentException("URI authority is empty: " + uri);
        }
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/");

        initVFSOperations();
    }

    private void initVFSOperations() {
        Options options = PaimonVirtualFileSystemConfiguration.convertToCatalogOptions(conf);
        String authority = uri.getAuthority();
        int delimiterIndex = authority.indexOf(".");
        if (delimiterIndex == 0 || delimiterIndex == authority.length() - 1) {
            throw new IllegalArgumentException("Invalid URI authority: " + uri);
        }
        if (delimiterIndex < 0) {
            // pvfs://catalog_name/database_name/table_name/file, so uri authority is catalog name
            options.set(CatalogOptions.WAREHOUSE, authority);
        } else {
            // pvfs://catalog_name.endpoint/database_name/table_name/file
            String catalogName = authority.substring(0, delimiterIndex);
            String endpoint = authority.substring(delimiterIndex + 1);
            options.set(CatalogOptions.WAREHOUSE, catalogName);
            options.set(RESTCatalogOptions.URI, endpoint);
        }

        // Set user agent
        options.set(RESTCatalogOptions.HTTP_USER_AGENT, USER_AGENT);

        vfsOperations = new VFSOperations(options);
    }

    private String getVirtualPath(Path path) {
        Path qualifiedPath = path.makeQualified(uri, workingDirectory);
        // convert to absolute path, like /database_name/table_name/file
        return qualifiedPath.toUri().getPath();
    }

    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException(
                    "Cannot create file for virtual path " + f + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new IOException(
                    "Cannot create file for virtual path " + f + " which is a database");
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            throw new IOException(
                    "Cannot create file for table level virtual path " + f + " which is a table");
        } else {
            VFSTableObjectIdentifier identifier = (VFSTableObjectIdentifier) vfsIdentifier;
            if (identifier.tableInfo() == null) {
                vfsOperations.createObjectTable(identifier.databaseName(), identifier.tableName());
                identifier =
                        (VFSTableObjectIdentifier)
                                vfsOperations.getVFSIdentifier(getVirtualPath(f));
            }
            PositionOutputStream out =
                    identifier
                            .tableInfo()
                            .fileIO()
                            .newOutputStream(identifier.filePath(), overwrite);
            return new FSDataOutputStream(out, statistics);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(path));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new FileNotFoundException(
                    "Cannot open file for virtual path " + path + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new FileNotFoundException(
                    "Cannot open file for virtual path " + path + " which is a database");
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            throw new FileNotFoundException(
                    "Cannot open file for virtual path " + path + " which is a table");
        } else {
            VFSTableObjectIdentifier identifier = (VFSTableObjectIdentifier) vfsIdentifier;
            VFSTableInfo tableInfo = identifier.tableInfo();
            if (tableInfo == null) {
                throw new IOException(
                        "Cannot open file for virtual path "
                                + path
                                + " which is not in an existing table");
            }
            VFSInputStream in =
                    new VFSInputStream(
                            tableInfo.fileIO().newInputStream(identifier.filePath()), statistics);
            return new FSDataInputStream(in);
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("Append is not supported");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        VFSIdentifier srcVfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(src));
        VFSIdentifier dstVfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(dst));
        if (srcVfsIdentifier instanceof VFSCatalogIdentifier) {
            LOG.debug("Rename root path is ignored");
            return false;
        } else if (srcVfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new IOException(
                    "Cannot rename from virtual path " + src + " which is a database");
        } else if (srcVfsIdentifier instanceof VFSTableRootIdentifier) {
            if (!(dstVfsIdentifier instanceof VFSTableRootIdentifier)) {
                throw new IOException(
                        "Cannot rename from table path " + src + " to non-table path " + dst);
            }
            return renameTable(
                    (VFSTableRootIdentifier) srcVfsIdentifier,
                    (VFSTableRootIdentifier) dstVfsIdentifier);
        } else {
            if (!(dstVfsIdentifier instanceof VFSTableIdentifier)) {
                throw new IOException(
                        "Cannot rename to virtual path " + dst + " which is not a table");
            }
            VFSTableObjectIdentifier srcIdentifier = (VFSTableObjectIdentifier) srcVfsIdentifier;
            VFSTableIdentifier dstIdentifier = (VFSTableIdentifier) dstVfsIdentifier;
            VFSTableInfo srcTableInfo = srcIdentifier.tableInfo();
            VFSTableInfo dstTableInfo = dstIdentifier.tableInfo();
            if (srcTableInfo == null) {
                throw new IOException(
                        "Cannot rename from virtual path "
                                + src
                                + " which is not in an existing table");
            }
            if (dstTableInfo == null) {
                throw new IOException(
                        "Cannot rename to virtual path "
                                + dst
                                + " which is not in an existing table");
            }
            if (!srcTableInfo.tableId().equals(dstTableInfo.tableId())) {
                throw new IOException(
                        "Cannot rename from virtual path "
                                + src
                                + " to virtual path "
                                + dst
                                + " which is not in the same table");
            }
            return srcTableInfo.fileIO().rename(srcIdentifier.filePath(), dstIdentifier.filePath());
        }
    }

    private boolean renameTable(
            VFSTableRootIdentifier srcIdentifier, VFSTableRootIdentifier dstIdentifier)
            throws IOException {
        if (!srcIdentifier.databaseName().equals(dstIdentifier.databaseName())) {
            throw new IOException("Do not support rename table with different database");
        }
        if (srcIdentifier.tableInfo() == null) {
            // return false if src does not exist
            LOG.debug(
                    "Source table not found {}.{}",
                    srcIdentifier.databaseName(),
                    srcIdentifier.tableName());
            return false;
        }
        if (srcIdentifier.tableName().equals(dstIdentifier.tableName())) {
            // src equals to dst, return true
            return true;
        }
        try {
            vfsOperations.renameTable(
                    srcIdentifier.databaseName(),
                    srcIdentifier.tableName(),
                    dstIdentifier.tableName());
            return true;
        } catch (FileNotFoundException e) {
            LOG.debug(
                    "Source table not found {}.{}",
                    srcIdentifier.databaseName(),
                    srcIdentifier.tableName());
            return false;
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException("Cannot delete virtual path " + f + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            String databaseName = ((VFSDatabaseIdentifier) vfsIdentifier).databaseName();
            try {
                vfsOperations.dropDatabase(databaseName, recursive);
            } catch (FileNotFoundException e) {
                LOG.debug("Database not found for deleting path " + f);
                return false;
            }
            return true;
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            VFSTableRootIdentifier vfsTableRootIdentifier = (VFSTableRootIdentifier) vfsIdentifier;
            try {
                vfsOperations.dropTable(
                        vfsTableRootIdentifier.databaseName(), vfsTableRootIdentifier.tableName());
            } catch (FileNotFoundException e) {
                LOG.debug("Table not found for deleting path " + f);
                return false;
            }
            return true;
        } else {
            VFSTableObjectIdentifier identifier = (VFSTableObjectIdentifier) vfsIdentifier;
            VFSTableInfo tableInfo = identifier.tableInfo();
            if (tableInfo == null) {
                throw new IOException(
                        "Cannot delete virtual path " + f + " which is not in an existing table");
            }
            return tableInfo.fileIO().delete(identifier.filePath(), recursive);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            return new FileStatus(0, true, 1, 0, 0, new Path(this.uri));
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            String databaseName = ((VFSDatabaseIdentifier) vfsIdentifier).databaseName();
            GetDatabaseResponse database = vfsOperations.getDatabase(databaseName);
            return convertDatabase(database);
        } else {
            VFSTableIdentifier identifier = (VFSTableIdentifier) vfsIdentifier;
            VFSTableInfo tableInfo = identifier.tableInfo();
            if (tableInfo == null) {
                throw new FileNotFoundException("Table not found for path " + f);
            }
            org.apache.paimon.fs.FileStatus fileStatus =
                    tableInfo.fileIO().getFileStatus(identifier.filePath());
            return convertFileStatus(
                    identifier.databaseName(), identifier.tableName(), tableInfo, fileStatus);
        }
    }

    private FileStatus convertDatabase(GetDatabaseResponse database) {
        return new FileStatus(0, true, 1, 0, 0, new Path(new Path(this.uri), database.getName()));
    }

    private FileStatus convertFileStatus(
            String databaseName,
            String tableName,
            VFSTableInfo tableInfo,
            org.apache.paimon.fs.FileStatus fileStatus)
            throws IOException {
        String tablePath = tableInfo.tablePath().toString();
        String filePath = fileStatus.getPath().toString();
        if (!filePath.startsWith(tablePath)) {
            throw new IOException(
                    "Result path " + filePath + " does not start with table location " + tablePath);
        }
        String childPath = filePath.substring(tablePath.length());
        if (!childPath.startsWith("/")) {
            childPath = "/" + childPath;
        }
        Path virtualPath = new Path(new Path(this.uri), databaseName + "/" + tableName + childPath);
        long blockSize = fileStatus.isDir() ? 0 : DEFAULT_BLOCK_SIZE;
        return new FileStatus(
                fileStatus.getLen(),
                fileStatus.isDir(),
                1,
                blockSize,
                fileStatus.getModificationTime(),
                virtualPath);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            List<String> databases = vfsOperations.listDatabases();
            return convertDatabases(databases);
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            String databaseName = ((VFSDatabaseIdentifier) vfsIdentifier).databaseName();
            List<String> tables = vfsOperations.listTables(databaseName);
            return convertTables(databaseName, tables);
        } else {
            VFSTableIdentifier identifier = (VFSTableIdentifier) vfsIdentifier;
            VFSTableInfo tableInfo = identifier.tableInfo();
            if (tableInfo == null) {
                throw new FileNotFoundException("Table not found for path " + f);
            }
            org.apache.paimon.fs.FileStatus[] fileStatuses =
                    tableInfo.fileIO().listStatus(identifier.filePath());
            return convertFileStatuses(
                    identifier.databaseName(), identifier.tableName(), tableInfo, fileStatuses);
        }
    }

    private FileStatus[] convertDatabases(List<String> databases) {
        FileStatus[] fileStatuses = new FileStatus[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            String database = databases.get(i);
            FileStatus fileStatus =
                    new FileStatus(0, true, 1, 0, 0, new Path(new Path(this.uri), database));
            fileStatuses[i] = fileStatus;
        }
        return fileStatuses;
    }

    private FileStatus[] convertTables(String database, List<String> tables) {
        FileStatus[] fileStatuses = new FileStatus[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            String table = tables.get(i);
            FileStatus fileStatus =
                    new FileStatus(
                            0, true, 1, 0, 0, new Path(new Path(this.uri), database + "/" + table));
            fileStatuses[i] = fileStatus;
        }
        return fileStatuses;
    }

    private FileStatus[] convertFileStatuses(
            String databaseName,
            String tableName,
            VFSTableInfo tableInfo,
            org.apache.paimon.fs.FileStatus[] fileStatuses)
            throws IOException {
        FileStatus[] virtualStatues = new FileStatus[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            virtualStatues[i] =
                    convertFileStatus(databaseName, tableName, tableInfo, fileStatuses[i]);
        }
        return virtualStatues;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        String virtualPath = getVirtualPath(f);
        // Hadoop TrashPolicy will mkdir /user/<root>/.Trash, and we should reject this operation
        // and return false, which indicates trash is not supported for TrashPolicy
        for (String component : virtualPath.split("/")) {
            if (component.equals(".Trash")) {
                LOG.info("PVFS do not support trash directory {}", f);
                return false;
            }
        }
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(virtualPath);
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException("Cannot mkdirs for virtual path " + f + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            String databaseName = ((VFSDatabaseIdentifier) vfsIdentifier).databaseName();
            vfsOperations.createDatabase(databaseName);
            return true;
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            VFSTableRootIdentifier identifier = (VFSTableRootIdentifier) vfsIdentifier;
            if (identifier.tableInfo() != null) {
                // Table already exists, no need to execute
                return true;
            }
            vfsOperations.createObjectTable(identifier.databaseName(), identifier.tableName());
            return true;
        } else {
            VFSTableObjectIdentifier identifier = (VFSTableObjectIdentifier) vfsIdentifier;
            if (identifier.tableInfo() == null) {
                vfsOperations.createObjectTable(identifier.databaseName(), identifier.tableName());
                identifier =
                        (VFSTableObjectIdentifier)
                                vfsOperations.getVFSIdentifier(getVirtualPath(f));
            }
            return identifier.tableInfo().fileIO().mkdirs(identifier.filePath());
        }
    }

    @Override
    public String getScheme() {
        return "pvfs";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDirectory = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    @VisibleForTesting
    public boolean isCacheEnabled() {
        return vfsOperations.isCacheEnabled();
    }
}
