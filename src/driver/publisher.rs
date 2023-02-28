use std::collections::HashMap;
use {
    crate::*,
    rdkafka::{
        error::KafkaError,
        producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    },
    std::time::Duration,
    models::{
        transactions::{TransactionDetail, TransactionModel},
    },
};
use crate::driver::config::DriverConfig;
use crate::driver::producer::Producer;

pub struct Publisher {
    producer: ThreadedProducer<DefaultProducerContext>,
    topics: HashMap<String, String>
}


impl Publisher {
    pub fn new() -> Self {
        let conf_map: DriverConfig = DriverConfig::read_from("./crates/indexer/config.json");
        Self {
            producer: Producer::new(conf_map.kafka).create(),
            topics: conf_map.topics
        }
    }

    pub fn send_txs(&self, txs: &[TransactionModel]) {
        let n = txs.len();
        let transaction_topic = self.topics["transaction_topic"].clone();
        for i in 0..n {
            let serialized_tx = serde_json::to_string(&txs[i]).unwrap();
            self.producer.send(BaseRecord::<Vec<u8>, _>::to(&transaction_topic).payload(serialized_tx.as_bytes())).expect("Failed to send message")
        }
    }
}
