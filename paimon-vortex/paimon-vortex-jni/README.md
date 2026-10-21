# Paimon Vortex JNI

This module provides Java JNI bindings for the [Vortex](https://github.com/spiraldb/vortex) columnar file format.

## Source Code Origin

The Java source code under `src/main/java/dev/vortex/` is adapted from the upstream
[vortex-jni](https://github.com/spiraldb/vortex/tree/develop/java/vortex-jni) module
(Apache License 2.0). The `dev.vortex.jni` package name is preserved to match the JNI
native method signatures in the pre-compiled Rust library.

The Java declarations must match the JNI signatures of the native library version in
`vortex.jni.version` (currently 0.89.0). A signature that differs still links, and then fails at
run time, so update both together.

Key adaptations from upstream:
- Build system changed from Gradle to Maven.
- Target Java version set to 1.8 for Paimon compatibility (upstream requires JDK 17).
- `java.lang.ref.Cleaner` (JDK 9+) replaced with explicit `close()` via `AutoCloseable`.
- Arrow memory allocation uses `Unsafe` allocator (via `arrow-memory-unsafe`) to guarantee
  buffer alignment required by Vortex's Rust FFI.
- Vortex exports strings and binaries as `Utf8View` and `BinaryView`. Arrow Java 15 cannot import
  view types, and newer Arrow Java versions need Java 11. `ArrowBatchStream` imports those columns
  as `Utf8` and `Binary` by copying their values, and moves all other columns without a copy.
- Only the parts of the upstream API that Paimon uses are kept.

## Native Library

The native library (`libvortex_jni`) is automatically extracted from the published
[`dev.vortex:vortex-jni`](https://mvnrepository.com/artifact/dev.vortex/vortex-jni)
Maven artifact during the `generate-resources` phase via `maven-dependency-plugin:unpack`.

The published jar includes pre-built binaries for:

| Platform              | Directory            | Library file            |
|-----------------------|----------------------|-------------------------|
| macOS Apple Silicon   | `darwin-aarch64`     | `libvortex_jni.dylib`   |
| Linux x86_64          | `linux-amd64`        | `libvortex_jni.so`      |
| Linux aarch64         | `linux-aarch64`      | `libvortex_jni.so`      |
| Windows x86_64        | `win-amd64`          | `libvortex_jni.dll`     |

No Rust toolchain or manual build is required.

## Verification

```bash
mvn test -pl paimon-vortex/paimon-vortex-jni,paimon-vortex/paimon-vortex-format \
    -Dcheckstyle.skip=true -Dspotless.check.skip=true
```

Tests that require the native library use `assumeTrue` to check availability at runtime.
If the library is not found, those tests will be skipped rather than fail.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `UnsatisfiedLinkError` | Ensure `maven-dependency-plugin:unpack` ran (check `target/classes/native/`). |
| `FileNotFoundException: Library not found` | Check `os.name` and `os.arch` system properties match a supported platform. |
| `Memory pointer ... not aligned` | Ensure `arrow.allocation.manager.type=Unsafe` is set (done automatically by `NativeLoader`). |
