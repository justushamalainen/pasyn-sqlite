//! Build script for pasyn-sqlite-core
//!
//! This build script handles:
//! - Compiling SQLite from the amalgamation source (bundled feature)
//! - Linking to system SQLite (system feature)
//! - Generating bindings configuration

use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Determine which SQLite to use
    let use_bundled = env::var("CARGO_FEATURE_BUNDLED").is_ok();
    let use_system = env::var("CARGO_FEATURE_SYSTEM").is_ok();

    if use_bundled && !use_system {
        build_bundled_sqlite(&out_dir);
    } else {
        link_system_sqlite();
    }

    // Tell cargo to rerun if these files change
    println!("cargo:rerun-if-changed=sqlite-src/sqlite3.c");
    println!("cargo:rerun-if-changed=sqlite-src/sqlite3.h");
    println!("cargo:rerun-if-changed=build.rs");
}

/// Build SQLite from the bundled amalgamation source
fn build_bundled_sqlite(out_dir: &PathBuf) {
    let sqlite_src = PathBuf::from("sqlite-src");
    let sqlite_c = sqlite_src.join("sqlite3.c");

    // Check if we have the amalgamation source
    if !sqlite_c.exists() {
        // If no sqlite3.c, fall back to system SQLite
        eprintln!("Warning: sqlite3.c not found in sqlite-src/, falling back to system SQLite");
        link_system_sqlite();
        return;
    }

    // Compile SQLite with recommended options for performance
    let mut build = cc::Build::new();

    build
        .file(&sqlite_c)
        .include(&sqlite_src)
        // Core options
        .define("SQLITE_ENABLE_FTS5", None)
        .define("SQLITE_ENABLE_RTREE", None)
        .define("SQLITE_ENABLE_JSON1", None)
        .define("SQLITE_ENABLE_COLUMN_METADATA", None)
        .define("SQLITE_ENABLE_STAT4", None)
        .define("SQLITE_ENABLE_UPDATE_DELETE_LIMIT", None)
        // Performance options
        .define("SQLITE_DQS", "0") // Disable double-quoted string literals
        .define("SQLITE_THREADSAFE", "1") // Enable multi-threading
        .define("SQLITE_DEFAULT_MEMSTATUS", "0") // Disable memory tracking
        .define("SQLITE_DEFAULT_WAL_SYNCHRONOUS", "1") // Normal sync in WAL mode
        .define("SQLITE_LIKE_DOESNT_MATCH_BLOBS", None)
        .define("SQLITE_MAX_EXPR_DEPTH", "0") // Unlimited expression depth
        .define("SQLITE_OMIT_DEPRECATED", None)
        .define("SQLITE_OMIT_PROGRESS_CALLBACK", None)
        .define("SQLITE_OMIT_SHARED_CACHE", None)
        .define("SQLITE_USE_ALLOCA", None)
        // Security
        .define("SQLITE_SECURE_DELETE", None)
        .define("SQLITE_ENABLE_DBPAGE_VTAB", None)
        .define("SQLITE_ENABLE_STMTVTAB", None)
        // Optimization
        .opt_level(3)
        .warnings(false);

    // Platform-specific settings
    #[cfg(unix)]
    {
        build.define("HAVE_USLEEP", "1");
        build.define("HAVE_LOCALTIME_R", "1");
        build.define("HAVE_GMTIME_R", "1");
        build.define("HAVE_FDATASYNC", "1");
    }

    #[cfg(target_os = "linux")]
    {
        build.define("HAVE_POSIX_FALLOCATE", "1");
        build.define("HAVE_MALLOC_USABLE_SIZE", "1");
    }

    #[cfg(target_os = "macos")]
    {
        build.define("HAVE_MALLOC_SIZE", "1");
    }

    build.compile("sqlite3");

    println!("cargo:rustc-link-lib=static=sqlite3");
    println!("cargo:rustc-link-search=native={}", out_dir.display());

    // Link math library on Unix
    #[cfg(unix)]
    println!("cargo:rustc-link-lib=m");

    // Link pthreads on Linux
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=pthread");

    // Link dl on Linux
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=dl");
}

/// Link to system SQLite using pkg-config
fn link_system_sqlite() {
    // Try pkg-config first
    if let Ok(lib) = pkg_config::Config::new()
        .atleast_version("3.20.0")
        .probe("sqlite3")
    {
        for path in &lib.link_paths {
            println!("cargo:rustc-link-search=native={}", path.display());
        }
        for lib in &lib.libs {
            println!("cargo:rustc-link-lib={}", lib);
        }
        return;
    }

    // Fallback: try linking directly
    println!("cargo:rustc-link-lib=sqlite3");

    // Common library paths
    #[cfg(target_os = "linux")]
    {
        println!("cargo:rustc-link-search=native=/usr/lib");
        println!("cargo:rustc-link-search=native=/usr/lib/x86_64-linux-gnu");
        println!("cargo:rustc-link-search=native=/usr/local/lib");
    }

    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    }
}
