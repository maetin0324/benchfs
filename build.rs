// Build script for BenchFS
//
// This script generates a pkg-config file for the BenchFS library,
// which IOR will use to find the library and header files.

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    // Generate pkg-config file
    generate_pkg_config();

    // Tell cargo to rerun if build.rs changes
    println!("cargo:rerun-if-changed=build.rs");
}

fn generate_pkg_config() {
    // Get output directory
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = PathBuf::from(&out_dir).join("benchfs.pc");

    // Get install prefix (default to /usr/local)
    let prefix = env::var("PREFIX").unwrap_or_else(|_| "/usr/local".to_string());

    // Get package version
    let version = env::var("CARGO_PKG_VERSION").unwrap();

    // Get manifest directory (project root)
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    // Get target directory for library path
    let profile = env::var("PROFILE").unwrap();
    let target_dir =
        env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| format!("{}/target", manifest_dir));
    let lib_dir = format!("{}/{}", target_dir, profile);

    // Include directory points to our header location
    let include_dir = format!("{}/ior_integration/benchfs_backend/include", manifest_dir);

    // Generate pkg-config content
    let pc_content = format!(
        r#"# BenchFS pkg-config file

prefix={prefix}
exec_prefix=${{prefix}}
libdir={lib_dir}
includedir={include_dir}

Name: BenchFS
Description: BenchFS distributed filesystem library
Version: {version}
Libs: -L${{libdir}} -lbenchfs -lpthread -ldl -lm
Cflags: -I${{includedir}}
"#,
        prefix = prefix,
        lib_dir = lib_dir,
        include_dir = include_dir,
        version = version
    );

    // Write to file
    let mut file = File::create(&dest_path).expect("Failed to create benchfs.pc");
    file.write_all(pc_content.as_bytes())
        .expect("Failed to write benchfs.pc");

    println!(
        "cargo:warning=Generated pkg-config file at: {}",
        dest_path.display()
    );
    println!("cargo:warning=To install:");
    println!(
        "cargo:warning=  sudo cp {} /usr/local/lib/pkgconfig/",
        dest_path.display()
    );
}
