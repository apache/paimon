package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

class MultiTableCommittableSerializerTest {
    private final CommitMessageSerializer fileSerializer = new CommitMessageSerializer();

    private final MultiTableCommittableSerializer serializer =
            new MultiTableCommittableSerializer(fileSerializer);

    @Test
    public void testFileMetadata() throws IOException {
        NewFilesIncrement newFilesIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        CommitMessage commitMessage =
                new CommitMessageImpl(row(0), 1, newFilesIncrement, compactIncrement);
        Committable committable = new Committable(9, Committable.Kind.FILE, commitMessage);
        String database = "database";
        String table = "table";
        String user = "user";
        MultiTableCommittable multiTableCommittable =
                MultiTableCommittable.fromCommittable(
                        Identifier.create(database, table), user, committable);
        Committable deserializeCommittable =
                serializer.deserialize(2, serializer.serialize(multiTableCommittable));

        assertThat(deserializeCommittable).isInstanceOf(MultiTableCommittable.class);

        assertThat(((MultiTableCommittable) deserializeCommittable).getDatabase())
                .isEqualTo(database);
        assertThat(((MultiTableCommittable) deserializeCommittable).getTable()).isEqualTo(table);
        assertThat(((MultiTableCommittable) deserializeCommittable).getCommitUser())
                .isEqualTo(user);
    }
}
