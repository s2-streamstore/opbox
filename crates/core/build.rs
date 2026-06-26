use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=OPBOX_BUILD_HASH");
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=../../Cargo.toml");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
    println!("cargo:rerun-if-changed=../client/Cargo.toml");
    println!("cargo:rerun-if-changed=../client/src");
    println!("cargo:rerun-if-changed=../daemon/Cargo.toml");
    println!("cargo:rerun-if-changed=../daemon/src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=src");

    let mut build_hash = std::env::var("OPBOX_BUILD_HASH")
        .or_else(|_| std::env::var("GITHUB_SHA"))
        .ok()
        .or_else(git_head)
        .unwrap_or_else(|| "unknown".to_string());
    if build_hash != "unknown" && git_dirty() {
        build_hash.push_str("-dirty");
    }

    println!("cargo:rustc-env=OPBOX_BUILD_HASH={build_hash}");
}

fn git_head() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let hash = String::from_utf8(output.stdout).ok()?;
    let hash = hash.trim();
    (!hash.is_empty()).then(|| hash.to_string())
}

fn git_dirty() -> bool {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output();
    let Ok(output) = output else {
        return false;
    };
    output.status.success() && !output.stdout.is_empty()
}
