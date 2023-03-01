use std::collections::HashMap;
use rdkafka::ClientConfig;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};

pub struct Producer {
    kafka_conf: HashMap<String, String>
}

impl Producer {
    pub fn new(kafka_conf: HashMap<String, String>) -> Self {
        Self {
            kafka_conf
        }
    }

    pub fn create(&self) -> ThreadedProducer<DefaultProducerContext> {
        let mut config = ClientConfig::new();
        for (k, v) in self.kafka_conf.iter() {
            config.set(k, v);
        }
        config.create().expect("Invalid producer config")
    }
}

