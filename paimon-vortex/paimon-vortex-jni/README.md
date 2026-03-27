# Paimon Vortex JNI

This module provides Java JNI bindings for the [Vortex](https://github.com/spiraldb/vortex) columnar file format.

## Source Code Origin

The Java source code under `src/main/java/dev/vortex/` is copied from the upstream
[vortex-jni](https://github.com/spiraldb/vortex/tree/develop/java/vortex-jni) module
(Apache License 2.0). The `dev.vortex.jni` package name is preserved to match the JNI
native method signatures in the pre-compiled Rust library.

Key adaptations from upstream:
- Build system changed from Gradle to Maven.
- Protobuf `.proto` files copied to `src/main/proto/` and compiled via `protobuf-maven-plugin`.
- Target Java version set to 1.8 for Paimon compatibility.

## Building the Native Library

The module requires a platform-specific native library (`libvortex_jni`) built from the
Vortex Rust project. This library is **not** published to Maven Central and must be built
from source.

### Prerequisites

- **Rust toolchain**: Install via [rustup](https://rustup.rs/)
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **C compiler**: `gcc` (Linux) or Xcode Command Line Tools (macOS).

### Build Steps

```bash
# 1. Clone the Vortex repository
git clone https://github.com/spiraldb/vortex.git
cd vortex

# 2. Build the native library
cargo build --package vortex-jni          # debug build
cargo build --package vortex-jni --release  # release build (optimized)

# 3. Copy to resources (example for macOS Apple Silicon)
mkdir -p /path/to/paimon-vortex-jni/src/main/resources/native/darwin-aarch64
cp target/debug/libvortex_jni.dylib \
   /path/to/paimon-vortex-jni/src/main/resources/native/darwin-aarch64/
```

Platform-specific paths:

| Platform              | Directory            | Library file            |
|-----------------------|----------------------|-------------------------|
| macOS Apple Silicon   | `darwin-aarch64`     | `libvortex_jni.dylib`   |
| macOS Intel           | `darwin-x86_64`      | `libvortex_jni.dylib`   |
| Linux x86_64          | `linux-amd64`        | `libvortex_jni.so`      |
| Linux aarch64         | `linux-aarch64`      | `libvortex_jni.so`      |

You only need to provide the library for your current platform.

### Expected Resources Layout

```
src/main/resources/native/
├── darwin-aarch64/
│   └── libvortex_jni.dylib
├── darwin-x86_64/
│   └── libvortex_jni.dylib
├── linux-amd64/
│   └── libvortex_jni.so
└── linux-aarch64/
    └── libvortex_jni.so
```

## Verification

```bash
mvn test -pl paimon-vortex/paimon-vortex-jni -Dcheckstyle.skip=true -Dspotless.check.skip=true
```

Tests that require the native library use `assumeTrue` to check availability at runtime.
If the library is not found, those tests will be skipped rather than fail.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `UnsatisfiedLinkError` | Ensure the `.dylib`/`.so` is in the correct `native/{os}-{arch}/` directory. |
| `FileNotFoundException: Library not found` | Check `os.name` and `os.arch` system properties match the directory name. |
| Rust compilation fails | Ensure Rust toolchain is up-to-date: `rustup update`. |
| Cross-compilation needed | Use `cargo build --target <triple>`, e.g. `x86_64-unknown-linux-gnu`. |
