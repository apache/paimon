package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** doc. */
public class IcebergMigrator implements Migrator {
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private final Catalog paimonCatalog;
    // TODO:paimon的FileIO应该和iceberg的FileIO区分开来？用各自的FileIO去读各自的文件
    private final FileIO fileIO;
    private final String paimonDatabaseName;
    private final String paimonTableNameame;

    private final IcebergPathFactory icebergPathFactory;
    private final IcebergMetadata icebergMetadata;

    public IcebergMigrator(
            Catalog paimonCatalog,
            IcebergPathFactory icebergPathFactory,
            String paimonDatabaseName,
            String paimonTableNameame) {
        this.paimonCatalog = paimonCatalog;
        this.fileIO = paimonCatalog.fileIO();
        this.paimonDatabaseName = paimonDatabaseName;
        this.paimonTableNameame = paimonTableNameame;

        this.icebergPathFactory = icebergPathFactory;
        this.icebergMetadata =
                IcebergMetadata.fromPath(
                        fileIO, icebergPathFactory.toMetadataPath(getIcebergNewestSnapshotId()));
    }

    @Override
    public void executeMigrate() throws Exception {

        Schema paimonSchema = icebergSchemaToPaimonSchema(icebergMetadata);
        Identifier paimonIdentifier = Identifier.create(paimonDatabaseName, paimonTableNameame);
        paimonCatalog.createDatabase(paimonDatabaseName, true);
        paimonCatalog.createTable(paimonIdentifier, paimonSchema, false);

        FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);

        IcebergManifestFile manifestFile =
                IcebergManifestFile.create(paimonTable, icebergPathFactory);
        IcebergManifestList manifestList =
                IcebergManifestList.create(paimonTable, icebergPathFactory);

        // get all IcebergManifestEntrys so that we can get all datafiles
        List<IcebergManifestFileMeta> icebergManifestFileMetas =
                manifestList.read(icebergMetadata.currentSnapshot().manifestList());
        List<IcebergManifestEntry> icebergEntries =
                icebergManifestFileMetas.stream()
                        .flatMap(
                                fileMeta ->
                                        manifestFile
                                                .read(new Path(fileMeta.manifestPath()).getName())
                                                .stream())
                        .filter(IcebergManifestEntry::isLive)
                        .collect(Collectors.toList());

        // construct paimon DataFileMeta from iceberg DataFileMeta
        List<IcebergDataFileMeta> icebergDataFileMetas =
                icebergEntries.stream()
                        .map(IcebergManifestEntry::file)
                        .collect(Collectors.toList());

        BinaryRow partitionRow = BinaryRow.EMPTY_ROW;
        Path newDir = paimonTable.store().pathFactory().bucketPath(partitionRow, 0);
        // TODO:需要获取到iceberg目前有几个分区，光有分区字段不够，需要真实的分区，如2000/12，2001/12这种
        if (!paimonTable.partitionKeys().isEmpty()) {
            newDir = paimonTable.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
        }
        Map<Path, Path> rollBack = new ConcurrentHashMap<>();
        List<DataFileMeta> dataFileMetas =
                FileMetaUtils.construct(
                        icebergDataFileMetas, fileIO, paimonTable, newDir, rollBack);
        CommitMessage commitMessage = FileMetaUtils.commitFile(partitionRow, dataFileMetas);
        try (BatchTableCommit commit = paimonTable.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(commitMessage));
        }
    }

    @Override
    public void deleteOriginTable(boolean delete) throws Exception {}

    @Override
    public void renameTable(boolean ignoreIfNotExists) throws Exception {}

    public int getIcebergNewestSnapshotId() {
        Path versionHintPath =
                new Path(icebergPathFactory.metadataDirectory(), VERSION_HINT_FILENAME);
        try {
            return Integer.parseInt(fileIO.readFileUtf8(versionHintPath));
        } catch (IOException e) {
            throw new RuntimeException(
                    "read iceberg version-hint.text failed. Iceberg metadata path: "
                            + icebergPathFactory.metadataDirectory());
        }
    }

    public Schema icebergSchemaToPaimonSchema(IcebergMetadata icebergMetadata) {
        // get iceberg current schema
        IcebergSchema icebergSchema =
                icebergMetadata.schemas().get(icebergMetadata.currentSchemaId());

        // get iceberg current partition spec
        int currentPartitionSpecId = icebergMetadata.defaultSpecId();
        IcebergPartitionSpec currentIcebergPartitionSpec =
                icebergMetadata.partitionSpecs().get(currentPartitionSpecId);

        List<DataField> dataFields =
                icebergSchema.fields().stream()
                        .map(IcebergDataField::toDatafield)
                        .collect(Collectors.toList());

        List<String> partitionKeys =
                currentIcebergPartitionSpec.fields().stream()
                        .map(IcebergPartitionField::name)
                        .collect(Collectors.toList());

        return new Schema(
                dataFields, partitionKeys, Collections.emptyList(), Collections.emptyMap(), null);
    }

    public void createPaimonTable() {}
}
