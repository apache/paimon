package org.apache.paimon.rest;

import java.io.IOException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

/**
 * A {@link org.apache.paimon.fs.FileIO} implementation for testing.
 *
 * <p>It is used to test the RESTFileIO.
 */
public class RESTTestFileIO extends LocalFileIO {
    private Options hadoopOptions;
    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        hadoopOptions = context.options();
        super.configure(context);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return super.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return super.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return super.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return super.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return super.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return super.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return super.rename(src, dst);
    }
}
