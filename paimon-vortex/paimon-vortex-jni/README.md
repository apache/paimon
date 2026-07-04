# Paimon Vortex JNI

This module provides Java JNI bindings for the [Vortex](https://github.com/spiraldb/vortex) columnar file format.

## Source Code Origin

The Java source code under `src/main/java/dev/vortex/` is adapted from the upstream
[vortex-jni](https://github.com/spiraldb/vortex/tree/develop/java/vortex-jni) module
(Apache License 2.0). The `dev.vortex.jni` package name is preserved to match the JNI
native method signatures in the pre-compiled Rust library.

Key adaptations from upstream:
- Build system changed from Gradle to Maven.
- Target Java version set to 1.8 for Paimon compatibility (upstream requires JDK 17).
- `java.lang.ref.Cleaner` (JDK 9+) replaced with explicit `close()` via `AutoCloseable`.
- Protobuf-based expression serialization replaced with native pointer-based `Expression` API.
- Arrow memory allocation uses `Unsafe` allocator (via `arrow-memory-unsafe`) to guarantee
  buffer alignment required by Vortex's Rust FFI.

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
