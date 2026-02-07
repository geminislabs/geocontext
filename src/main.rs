mod circuit_breaker;
mod config;
mod domain;
mod enrichers;
mod input;
mod kafka;
mod models;
mod output;
mod pipeline;

use anyhow::Result;
use circuit_breaker::{CircuitBreaker, Config as BreakerConfig};
use config::Config;
use input::InputConsumer;
use kafka::{KafkaConsumer, KafkaProducer};
use output::OutputProducer;
use pipeline::Processor;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!("Starting geocontext microservice with hexagonal architecture");

    let config = Config::from_env()?;
    info!("Configuration loaded successfully");

    // Capa de infraestructura: Kafka consumer y producer
    let consumer_cb = Arc::new(CircuitBreaker::new(
        "kafka-consumer",
        BreakerConfig::new(
            config.circuit_breaker.failure_threshold,
            config.circuit_breaker_duration(),
        ),
    ));

    let producer_cb = Arc::new(CircuitBreaker::new(
        "kafka-producer",
        BreakerConfig::new(
            config.circuit_breaker.failure_threshold,
            config.circuit_breaker_duration(),
        ),
    ));

    info!("Initializing infrastructure layer (Kafka)");
    let kafka_consumer = KafkaConsumer::new(&config.kafka, consumer_cb)?;
    let kafka_producer = KafkaProducer::new(&config.kafka, producer_cb)?;

    // Capa de adaptadores: Input y Output
    info!("Initializing adapter layer (Input/Output)");
    let input = InputConsumer::new(kafka_consumer);
    let output = OutputProducer::new(kafka_producer, config.kafka.output_topic.clone());

    // Capa de aplicación: Pipeline processor
    info!("Initializing application layer (Pipeline)");
    let processor = Processor::new(
        input, 
        output, 
        config.commit_on_produce_success,
        config.h3_resolution,
    );

    info!("All layers initialized, starting processing loop");
    info!("Architecture: Infrastructure (Kafka) → Adapters (Input/Output) → Pipeline");

    if let Err(e) = processor.run().await {
        error!(error = ?e, "Fatal error in processing pipeline");
        return Err(e);
    }

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,geocontext=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();
}
