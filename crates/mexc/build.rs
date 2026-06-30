fn main() {
    let proto_root = std::path::Path::new("proto");
    let proto_files = [
        "PublicAggreDealsV3Api.proto",
        "PublicAggreDepthsV3Api.proto",
        "PublicIncreaseDepthsV3Api.proto",
        "PublicIncreaseDepthsBatchV3Api.proto",
        "PublicLimitDepthsV3Api.proto",
        "PublicBookTickerV3Api.proto",
        "PublicBookTickerBatchV3Api.proto",
        "PublicAggreBookTickerV3Api.proto",
        "PublicSpotKlineV3Api.proto",
        "PublicMiniTickerV3Api.proto",
        "PublicMiniTickersV3Api.proto",
        "PushDataV3ApiWrapper.proto",
    ];

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    // SAFETY: build scripts run in a controlled single-process context for this crate.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    let protos: Vec<_> = proto_files
        .iter()
        .map(|file| proto_root.join(file))
        .collect();

    let mut config = prost_build::Config::new();
    config.include_file("mexc_spot_protos.rs");
    config
        .compile_protos(&protos, &[proto_root])
        .expect("compile protos");
}
