package org.apache.paimon.rest;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;

/**
 * RESTFileIOTestLoader for testing.
 */
public class RESTFileIOTestLoader implements FileIOLoader {

    public static final String SCHEME = "rest-test-file-io";

    private static final long serialVersionUID = 1L;

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileIO load(Path path) {
        return new RESTTestFileIO();
    }
}
