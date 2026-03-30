# Paimon Tantivy JNI

JNI wrapper around [Tantivy](https://github.com/quickwit-oss/tantivy) full-text search engine. Fixed schema: `rowId` (long) + `text` (String). Search returns top-N results with scores for ranking.

## Build

### 1. Build the Rust native library

```bash
cd rust
cargo build --release
```

Output artifacts in `rust/target/release/`:
- macOS: `libtantivy_jni.dylib`
- Linux: `libtantivy_jni.so`

### 2. Copy native library to resources

```bash
# macOS (Apple Silicon)
mkdir -p src/main/resources/native/darwin-aarch64
cp rust/target/release/libtantivy_jni.dylib src/main/resources/native/darwin-aarch64/

# macOS (Intel)
mkdir -p src/main/resources/native/darwin-x86_64
cp rust/target/release/libtantivy_jni.dylib src/main/resources/native/darwin-x86_64/

# Linux (x86_64)
mkdir -p src/main/resources/native/linux-amd64
cp rust/target/release/libtantivy_jni.so src/main/resources/native/linux-amd64/
```

### 3. Build the Java module

```bash
# From the project root
mvn compile -pl paimon-tantivy/paimon-tantivy-jni -am
```

## Usage

```java
// Create index and write documents
try (TantivyIndexWriter writer = new TantivyIndexWriter("/tmp/my_index")) {
    writer.addDocument(1L, "Apache Paimon is a streaming data lake platform");
    writer.addDocument(2L, "Tantivy is a full-text search engine written in Rust");
    writer.addDocument(3L, "Paimon supports real-time data ingestion");
    writer.commit();
}

// Search — returns (rowId, score) pairs ranked by relevance
try (TantivySearcher searcher = new TantivySearcher("/tmp/my_index")) {
    SearchResult result = searcher.search("paimon", 10);
    for (int i = 0; i < result.size(); i++) {
        System.out.println("rowId=" + result.getRowIds()[i]
            + " score=" + result.getScores()[i]);
    }
}
```
