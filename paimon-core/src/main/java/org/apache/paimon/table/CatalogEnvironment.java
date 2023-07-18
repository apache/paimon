package org.apache.paimon.table;

import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.Lock;

import javax.annotation.Nullable;

public class CatalogEnvironment {
    private final Lock.Factory lockFactory;
    @Nullable private final MetastoreClient.Factory metastoreClientFactory;
    @Nullable private final LineageMeta lineageMeta;

    public CatalogEnvironment(
            Lock.Factory lockFactory,
            @Nullable MetastoreClient.Factory metastoreClientFactory,
            @Nullable LineageMeta lineageMeta) {
        this.lockFactory = lockFactory;
        this.metastoreClientFactory = metastoreClientFactory;
        this.lineageMeta = lineageMeta;
    }

    public Lock.Factory lockFactory() {
        return lockFactory;
    }

    @Nullable
    public MetastoreClient.Factory metastoreClientFactory() {
        return metastoreClientFactory;
    }

    @Nullable
    public LineageMeta lineageMeta() {
        return lineageMeta;
    }
}
