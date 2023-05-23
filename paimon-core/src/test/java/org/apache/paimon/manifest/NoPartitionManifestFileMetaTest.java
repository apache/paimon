package org.apache.paimon.manifest;

import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

public class NoPartitionManifestFileMetaTest extends ManifestFileMetaTestBase {
    private  final RowType noPartitionType = RowType.of();

    @TempDir
    java.nio.file.Path tempDir;
    private ManifestFile manifestFile;


    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @RepeatedTest(3)
    public void testMerge() {
        List<ManifestFileMeta> input = createBaseManifestFileMetas(false);
        addDeltaManifests(input,false);

        List<ManifestFileMeta> merged =
                ManifestFileMeta.merge(input, manifestFile, 500, 3, 200, getPartitionType());
        assertEquivalentEntries(input, merged);
    }

    @Override
    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    public RowType getPartitionType() {
        return noPartitionType;
    }
}
