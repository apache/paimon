package org.apache.flink.table.store.file.catalog;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;

public abstract class AbstractCatalog implements Catalog {

    protected static final String DB_SUFFIX = ".db";

    @Override
    public Path getTableLocation(ObjectPath tablePath) {
        return new Path(databasePath(tablePath.getDatabaseName()), tablePath.getObjectName());
    }

    protected Path databasePath(String database) {
        return new Path(warehouse(), database + DB_SUFFIX);
    }

    protected abstract String warehouse();
}
