fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/kv.proto"], &["proto"]) 
        .expect("failed to compile protos");
}
