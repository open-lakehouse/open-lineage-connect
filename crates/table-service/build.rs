fn main() {
    // Tell Cargo to rebuild whenever any proto in the export tree changes.
    // `connectrpc-build` doesn't emit these instructions itself, so without
    // them edits to `proto-export/**.proto` (e.g. after `make proto-export`)
    // would not invalidate the build script's output and the generated
    // Rust types would go stale.
    println!("cargo:rerun-if-changed=build.rs");
    for entry in walk_protos("proto-export") {
        println!("cargo:rerun-if-changed={}", entry);
    }

    connectrpc_build::Config::new()
        .files(&[
            "proto-export/lineage/v1/lineage.proto",
            "proto-export/table/v1/table_writer.proto",
        ])
        .includes(&["proto-export"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}

fn walk_protos(root: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![std::path::PathBuf::from(root)];
    while let Some(dir) = stack.pop() {
        let read = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in read.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("proto") {
                if let Some(s) = path.to_str() {
                    out.push(s.to_string());
                }
            }
        }
    }
    out
}
