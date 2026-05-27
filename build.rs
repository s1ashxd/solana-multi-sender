fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=LIBTPA_PATH");
    if std::env::var("CARGO_FEATURE_LIBTPA").is_err() {
        return;
    }
    let root = std::env::var("LIBTPA_PATH").unwrap_or_else(|_| "/home/s1ash/libtpa".to_string());
    let build_dir = std::path::Path::new(&root).join("build");
    println!("cargo:rustc-link-search=native={}", build_dir.display());
    let dpdk = build_dir.join("dpdk/v20.11.3/x86_64-native-linux-gcc/lib");
    if dpdk.exists() {
        println!("cargo:rustc-link-search=native={}", dpdk.display());
    }
    if build_dir.join("libtpa.a").exists() {
        println!("cargo:rustc-link-lib=static=tpa");
    } else {
        println!("cargo:rustc-link-lib=dylib=tpa");
    }
    for sys in ["numa", "dl", "pthread", "rt", "m"] {
        println!("cargo:rustc-link-lib=dylib={sys}");
    }
}
