fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["proto/metrics_service.proto"], &["proto"])?;
    Ok(())
}
