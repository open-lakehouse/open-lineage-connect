fn main() {
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
