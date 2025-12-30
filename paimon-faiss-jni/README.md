# Paimon Faiss JNI

Java bindings for [Faiss](https://github.com/facebookresearch/faiss) - A library for efficient similarity search and clustering of dense vectors.

## Overview

Paimon Faiss JNI provides a high-performance Java API for Faiss, following a similar architecture to [RocksDB Java](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics). The library consists of:

1. **Java API Layer** (`org.apache.paimon.faiss` package) - High-level Java classes for creating and managing Faiss indexes
2. **JNI Bridge** - C++ code that connects Java to the native Faiss library
3. **Native Libraries** - Pre-compiled Faiss libraries for different platforms, bundled in the JAR

## Features

- **Multiple Index Types**: Flat, IVF, HNSW, PQ, and more
- **Cross-Platform**: Supports Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64)
- **Fat JAR**: All native libraries bundled in a single JAR
- **Automatic Native Library Loading**: No manual library path configuration required
- **Thread-Safe Loading**: Safe for use in multi-threaded applications

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-faiss-jni</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Basic Usage

```java
import org.apache.paimon.faiss.*;

// Create a flat index for exact nearest neighbor search
try (Index index = IndexFactory.createFlat(128, MetricType.L2)) {
    // Add vectors (10000 vectors of dimension 128)
    float[] vectors = new float[10000 * 128];
    // ... fill vectors with data ...
    index.add(vectors);
    
    // Search for 10 nearest neighbors
    float[] query = new float[128];
    // ... fill query vector ...
    SearchResult result = index.searchSingle(query, 10);
    
    // Access results
    for (int i = 0; i < 10; i++) {
        long id = result.getLabel(0, i);
        float distance = result.getDistance(0, i);
        System.out.println("Neighbor " + i + ": id=" + id + ", distance=" + distance);
    }
}
```

## Index Types

### Flat Index (Exact Search)

```java
// L2 distance (Euclidean)
Index index = IndexFactory.createFlat(dimension, MetricType.L2);

// Inner product (cosine similarity for normalized vectors)
Index index = IndexFactory.createFlat(dimension, MetricType.INNER_PRODUCT);
```

### HNSW Index (Approximate Search)

```java
// Create HNSW index with M=32 neighbors
Index index = IndexFactory.createHNSW(dimension, 32, MetricType.L2);

// Configure search parameters
IndexHNSW.setEfSearch(index, 64);  // Higher = more accurate but slower
```

### IVF Index (Approximate Search with Training)

```java
// Create IVF index with 1000 clusters
Index index = IndexFactory.createIVFFlat(dimension, 1000, MetricType.L2);

// Train the index (required before adding vectors)
index.train(trainingVectors);

// Add vectors
index.add(vectors);

// Configure search
IndexIVF.setNprobe(index, 10);  // Search 10 out of 1000 clusters
```

### Custom Index Description

```java
// Use Faiss index factory syntax
Index index = IndexFactory.create(dimension, "IVF1000,PQ16", MetricType.L2);
```

## Index Operations

### Adding Vectors

```java
// Add multiple vectors
float[] vectors = new float[n * dimension];
index.add(vectors);

// Add single vector
float[] vector = new float[dimension];
index.addSingle(vector);

// Add with custom IDs (requires IDMap index)
Index index = IndexFactory.createFlatWithIds(dimension, MetricType.L2);
long[] ids = {100, 200, 300};
index.addWithIds(vectors, ids);
```

### Searching

```java
// Single query, k neighbors
SearchResult result = index.searchSingle(query, k);

// Batch queries
float[] queries = new float[numQueries * dimension];
SearchResult result = index.search(queries, k);

// Access results
for (int q = 0; q < numQueries; q++) {
    long[] labels = result.getLabelsForQuery(q);
    float[] distances = result.getDistancesForQuery(q);
}
```

### Persistence

```java
// Save to file
index.writeToFile("/path/to/index.faiss");

// Load from file
Index loaded = Index.readFromFile("/path/to/index.faiss");

// Serialize to byte array
byte[] data = index.serialize();

// Deserialize from byte array
Index restored = Index.deserialize(data);
```

## Building from Source

### Prerequisites

- JDK 8 or later
- Maven 3.6+
- CMake 3.14+
- C++ compiler with C++17 support
- Faiss library (install via package manager or build from source)
- OpenMP

### Build Native Library

```bash
# Install Faiss (example for Ubuntu)
sudo apt-get install libfaiss-dev

# Or build Faiss from source
git clone https://github.com/facebookresearch/faiss.git
cd faiss
cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF
cmake --build build -j
sudo cmake --install build

# Build native library
./scripts/build-native.sh

# Build Java JAR
mvn package
```

### Cross-Platform Builds

For cross-platform builds, use the GitHub Actions workflow or Docker:

```bash
# Build using Docker (Linux x86_64)
docker run --rm -v $PWD:/work dockcross/linux-x64 bash -c \
  "cd /work && ./scripts/build-native.sh"
```

## Configuration

### Native Library Path

By default, the library loads native libraries from the JAR. You can override this:

```java
// Use system property
System.setProperty("paimon.faiss.lib.path", "/path/to/libpaimon_faiss_jni.so");

// Or rely on java.library.path
java -Djava.library.path=/path/to/libs -jar myapp.jar
```

### Thread Count

```java
// Set number of threads for parallel operations
Faiss.setNumThreads(4);

// Get current thread count
int threads = Faiss.getNumThreads();
```

## API Reference

### Core Classes

| Class | Description |
|-------|-------------|
| `Index` | Main index class for vector storage and search |
| `IndexFactory` | Factory methods for creating different index types |
| `SearchResult` | Container for k-NN search results |
| `RangeSearchResult` | Container for range search results |
| `MetricType` | Enum for L2 and Inner Product metrics |

### Utility Classes

| Class | Description |
|-------|-------------|
| `Faiss` | Global configuration (thread count, version) |
| `IndexIVF` | IVF-specific operations (nprobe, nlist) |
| `IndexHNSW` | HNSW-specific operations (efSearch) |
| `NativeLibraryLoader` | Handles native library loading |

## Performance Tips

1. **Choose the right index type**:
   - Small dataset (<10K vectors): Use `Flat`
   - Medium dataset (10K-1M): Use `HNSW` or `IVFFlat`
   - Large dataset (>1M): Use `IVFPQ` or `IVF+HNSW`

2. **Batch operations**: Add and search in batches for better performance

3. **Training**: For IVF indexes, use representative training data (typically 10-100x the number of clusters)

4. **Tune parameters**:
   - IVF: Increase `nprobe` for better recall
   - HNSW: Increase `efSearch` for better recall

## License

Apache License 2.0

## Acknowledgments

- [Faiss](https://github.com/facebookresearch/faiss) by Facebook AI Research
- [RocksDB Java](https://github.com/facebook/rocksdb) for the JNI architecture inspiration

