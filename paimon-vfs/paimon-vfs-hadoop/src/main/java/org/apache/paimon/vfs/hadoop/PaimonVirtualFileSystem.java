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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.vfs.TableNotFoundException;
import org.apache.paimon.vfs.VFSCatalogIdentifier;
import org.apache.paimon.vfs.VFSDatabaseIdentifier;
import org.apache.paimon.vfs.VFSIdentifier;
import org.apache.paimon.vfs.VFSOperations;
import org.apache.paimon.vfs.VFSTableIdentifier;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/** Paimon virtual file system. */
public class PaimonVirtualFileSystem extends FileSystem {
    private Path workingDirectory;
    private URI uri;
    private VFSOperations vfsOperations;
    private Configuration conf;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        setConf(conf);
        this.conf = conf;
        super.initialize(uri, conf);

        this.workingDirectory = new Path(uri);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/");

        initVFSOperations();
    }

    private void initVFSOperations() {
        Options options = new Options();
        String fsUri = getConf().get(PaimonVirtualFileSystemConfiguration.FS_URI);
        if (fsUri == null) {
            throw new IllegalArgumentException("fs.pas.uri is not configured");
        } else {
            options.set(RESTCatalogOptions.URI, fsUri);
        }
        String tokenProvider =
                getConf().get(PaimonVirtualFileSystemConfiguration.FS_TOKEN_PROVIDER);
        if (tokenProvider == null) {
            throw new IllegalArgumentException("fs.pas.token.provider is not configured");
        } else {
            options.set(RESTCatalogOptions.TOKEN_PROVIDER, tokenProvider);
        }
        String token = getConf().get(PaimonVirtualFileSystemConfiguration.FS_TOKEN);
        if (token != null) {
            options.set(RESTCatalogOptions.TOKEN, token);
        }
        String accessKeyId =
                getConf().get(PaimonVirtualFileSystemConfiguration.FS_DLF_ACCESS_KEY_ID);
        String accessKeySecret =
                getConf().get(PaimonVirtualFileSystemConfiguration.FS_DLF_ACCESS_KEY_SECRET);
        String tokenLoader =
                getConf().get(PaimonVirtualFileSystemConfiguration.FS_DLF_TOKEN_LOADER);
        if (tokenLoader != null) {
            options.set(RESTCatalogOptions.DLF_TOKEN_LOADER, tokenLoader);
        } else if (accessKeyId != null && accessKeySecret != null) {
            options.set(RESTCatalogOptions.DLF_ACCESS_KEY_ID, accessKeyId);
            options.set(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET, accessKeySecret);
        }
        String regionId = getConf().get(PaimonVirtualFileSystemConfiguration.FS_DLF_REGION);
        if (regionId != null) {
            options.set(RESTCatalogOptions.DLF_REGION, regionId);
        }
        // pas://catalog_name/database_name/table_name/file, so uri authority is catalog name
        options.set(CatalogOptions.WAREHOUSE, uri.getAuthority());

        CatalogContext catalogContext = CatalogContext.create(options);
        vfsOperations = new VFSOperations(catalogContext);
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
            VFSTableObjectIdentifier vfsTableObjectIdentifier =
                    (VFSTableObjectIdentifier) vfsIdentifier;
            if (!vfsTableObjectIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot create a file for virtual path "
                                + f
                                + " which is not in an existing table");
            }
            PositionOutputStream out =
                    vfsTableObjectIdentifier
                            .fileIO()
                            .newOutputStream(vfsTableObjectIdentifier.getRealPath(), overwrite);
            return new FSDataOutputStream(out, statistics);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(path));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException(
                    "Cannot open file for virtual path " + path + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new IOException(
                    "Cannot open file for virtual path " + path + " which is a database");
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            throw new IOException(
                    "Cannot open file for table level virtual path " + path + " which is a table");
        } else {
            VFSTableObjectIdentifier vfsTableObjectIdentifier =
                    (VFSTableObjectIdentifier) vfsIdentifier;
            if (!vfsTableObjectIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot open file for virtual path "
                                + path
                                + " which is not in an existing table");
            }
            VFSInputStream in =
                    new VFSInputStream(
                            vfsTableObjectIdentifier
                                    .fileIO()
                                    .newInputStream(vfsTableObjectIdentifier.getRealPath()));
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
            throw new IOException("Cannot rename from virtual path " + src + " which is a catalog");
        } else if (srcVfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new IOException(
                    "Cannot rename from virtual path " + src + " which is a database");
        } else if (srcVfsIdentifier instanceof VFSTableRootIdentifier) {
            throw new IOException("Cannot rename from virtual path " + src + " which is a table");
        } else {
            if (!(dstVfsIdentifier instanceof VFSTableIdentifier)) {
                throw new IOException(
                        "Cannot rename to virtual path " + dst + " which is not a table");
            }
            VFSTableIdentifier srcTableIdentifier = (VFSTableIdentifier) srcVfsIdentifier;
            VFSTableIdentifier dstTableIdentifier = (VFSTableIdentifier) dstVfsIdentifier;
            if (!srcTableIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot rename from virtual path "
                                + src
                                + " which is not in an existing table");
            }
            if (!dstTableIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot rename to virtual path "
                                + dst
                                + " which is not in an existing table");
            }
            TableMetadata srcTable = srcTableIdentifier.getTable();
            TableMetadata dstTable = dstTableIdentifier.getTable();
            if (!srcTable.uuid().equals(dstTable.uuid())) {
                throw new IOException(
                        "Cannot rename from virtual path "
                                + src
                                + " to virtual path "
                                + dst
                                + " which is not in the same table");
            }
            return srcTableIdentifier
                    .fileIO()
                    .rename(srcTableIdentifier.getRealPath(), dstTableIdentifier.getRealPath());
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException("Cannot delete virtual path " + f + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            throw new IOException("Cannot delete virtual path " + f + " which is a database");
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            throw new IOException("Cannot delete virtual path " + f + " which is a table");
        } else {
            VFSTableObjectIdentifier vfsTableObjectIdentifier =
                    (VFSTableObjectIdentifier) vfsIdentifier;
            if (!vfsTableObjectIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot delete virtual path " + f + " which is not in an existing table");
            }
            return vfsTableObjectIdentifier
                    .fileIO()
                    .delete(vfsTableObjectIdentifier.getRealPath(), recursive);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            return new FileStatus(0, true, 1, 1, 0, new Path(this.uri));
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            Database database = vfsOperations.getDatabase(vfsIdentifier.getDatabaseName());
            return convertDatabase(database);
        } else {
            VFSTableIdentifier vfsTableIdentifier = (VFSTableIdentifier) vfsIdentifier;
            if (!vfsTableIdentifier.isTableExist()) {
                throw new TableNotFoundException("Table not found for path " + f);
            }
            org.apache.paimon.fs.FileStatus fileStatus =
                    vfsTableIdentifier.fileIO().getFileStatus(vfsTableIdentifier.getRealPath());
            return convertFileStatus(vfsTableIdentifier, fileStatus);
        }
    }

    private FileStatus convertDatabase(Database database) {
        return new FileStatus(0, true, 1, 1, 0, new Path(new Path(this.uri), database.name()));
    }

    private FileStatus convertFileStatus(
            VFSTableIdentifier vfsIdentifier, org.apache.paimon.fs.FileStatus paimonFileStatus)
            throws IOException {
        String realPath = paimonFileStatus.getPath().toString();
        if (!realPath.startsWith(vfsIdentifier.getTableLocation())) {
            throw new IOException(
                    "Result path "
                            + realPath
                            + " does not start with table location "
                            + vfsIdentifier.getTableLocation());
        }
        String childPath = realPath.substring(vfsIdentifier.getTableLocation().length());
        if (!childPath.startsWith("/")) {
            childPath = "/" + childPath;
        }
        Path virtualPath =
                new Path(
                        new Path(this.uri),
                        vfsIdentifier.getDatabaseName()
                                + "/"
                                + vfsIdentifier.getTableName()
                                + childPath);
        return new FileStatus(
                paimonFileStatus.getLen(),
                paimonFileStatus.isDir(),
                1,
                1,
                paimonFileStatus.getModificationTime(),
                virtualPath);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            List<String> databases = vfsOperations.listDatabases();
            return convertDatabases(databases);
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            List<String> tables = vfsOperations.listTables(vfsIdentifier.getDatabaseName());
            return convertTables(vfsIdentifier.getDatabaseName(), tables);
        } else {
            VFSTableIdentifier vfsTableIdentifier = (VFSTableIdentifier) vfsIdentifier;
            if (!vfsTableIdentifier.isTableExist()) {
                throw new TableNotFoundException("Table not found for path " + f);
            }
            org.apache.paimon.fs.FileStatus[] paimonFileStatuses =
                    vfsTableIdentifier.fileIO().listStatus(vfsTableIdentifier.getRealPath());
            return convertFileStatuses(vfsTableIdentifier, paimonFileStatuses);
        }
    }

    private FileStatus[] convertDatabases(List<String> databases) {
        FileStatus[] fileStatuses = new FileStatus[databases.size()];
        for (int i = 0; i < databases.size(); i++) {
            String database = databases.get(i);
            FileStatus fileStatus =
                    new FileStatus(0, true, 1, 1, 0, new Path(new Path(this.uri), database));
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
                            0, true, 1, 1, 0, new Path(new Path(this.uri), database + "/" + table));
            fileStatuses[i] = fileStatus;
        }
        return fileStatuses;
    }

    private FileStatus[] convertFileStatuses(
            VFSTableIdentifier vfsIdentifier, org.apache.paimon.fs.FileStatus[] paimonFileStatuses)
            throws IOException {
        FileStatus[] fileStatuses = new FileStatus[paimonFileStatuses.length];
        for (int i = 0; i < paimonFileStatuses.length; i++) {
            fileStatuses[i] = convertFileStatus(vfsIdentifier, paimonFileStatuses[i]);
        }
        return fileStatuses;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        VFSIdentifier vfsIdentifier = vfsOperations.getVFSIdentifier(getVirtualPath(f));
        if (vfsIdentifier instanceof VFSCatalogIdentifier) {
            throw new IOException("Cannot mkdirs for virtual path " + f + " which is a catalog");
        } else if (vfsIdentifier instanceof VFSDatabaseIdentifier) {
            vfsOperations.createDatabase(vfsIdentifier.getDatabaseName());
            return true;
        } else if (vfsIdentifier instanceof VFSTableRootIdentifier) {
            VFSTableRootIdentifier vfsTableRootIdentifier = (VFSTableRootIdentifier) vfsIdentifier;
            if (vfsTableRootIdentifier.isTableExist()) {
                // Table already exists, no need to execute
                return true;
            }
            vfsOperations.createObjectTable(
                    vfsIdentifier.getDatabaseName(), vfsTableRootIdentifier.getTableName());
            return true;
        } else {
            VFSTableObjectIdentifier vfsTableObjectIdentifier =
                    (VFSTableObjectIdentifier) vfsIdentifier;
            if (!vfsTableObjectIdentifier.isTableExist()) {
                throw new IOException(
                        "Cannot mkdirs for virtual path "
                                + f
                                + " which is not in an existing table");
            }
            return vfsTableObjectIdentifier.fileIO().mkdirs(vfsTableObjectIdentifier.getRealPath());
        }
    }

    @Override
    public String getScheme() {
        return "pas";
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
}
