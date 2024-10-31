package org.apache.paimon.lookup.sort;

import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.CacheManager.SegmentContainer;
import org.apache.paimon.memory.MemorySegment;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Cache for block reading. */
public class BlockCache implements Closeable {

    private final RandomAccessFile file;
    private final FileChannel channel;
    private final CacheManager cacheManager;
    private final Map<CacheKey, SegmentContainer> blocks;

    public BlockCache(RandomAccessFile file, CacheManager cacheManager) {
        this.file = file;
        this.channel = this.file.getChannel();
        this.cacheManager = cacheManager;
        this.blocks = new HashMap<>();
    }

    // TODO separate index and data cache
    private byte[] readFrom(long offset, int length) throws IOException {
        byte[] buffer = new byte[length];
        int read = channel.read(ByteBuffer.wrap(buffer), offset);

        if (read != length) {
            throw new IOException("Could not read all the data");
        }
        return buffer;
    }

    public MemorySegment getBlock(
            long position, int length, Function<byte[], byte[]> decompressFunc) {

        CacheKey cacheKey = CacheKey.forPosition(file, position, length);

        SegmentContainer container = blocks.get(cacheKey);
        if (container == null || container.getAccessCount() == CacheManager.REFRESH_COUNT) {
            MemorySegment segment =
                    cacheManager.getPage(
                            cacheKey,
                            key -> {
                                byte[] bytes = readFrom(position, length);
                                return decompressFunc.apply(bytes);
                            },
                            blocks::remove);
            container = new SegmentContainer(segment);
            blocks.put(cacheKey, container);
        }
        return container.access();
    }

    @Override
    public void close() throws IOException {
        Set<CacheKey> sets = new HashSet<>(blocks.keySet());
        for (CacheKey key : sets) {
            cacheManager.invalidPage(key);
        }
    }
}
