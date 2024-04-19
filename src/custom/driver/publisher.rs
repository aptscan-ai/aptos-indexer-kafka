use std::collections::HashMap;
use serde::Serialize;
use poem_openapi::types::ToJSON;

use {
    rdkafka::{
        producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    },
};

use crate::custom::driver::config::DriverConfig;
use crate::custom::driver::producer::Producer;
use aptos_api_types::Transaction;

pub struct Publisher {
    producer: ThreadedProducer<DefaultProducerContext>,
    topics: HashMap<String, String>,
    model_to_topic: HashMap<&'static str, &'static str>,
}


impl Publisher {
    pub fn new() -> Self {
        let conf_map: DriverConfig = DriverConfig::read_from("crates/indexer/config.json");
        Self {
            producer: Producer::new(conf_map.kafka).create(),
            topics: conf_map.topics,
            model_to_topic: HashMap::from([
                ("TransactionModel", "transaction_topic"),
                ("CoinInfo", "coin_info_topic"),
                ("CurrentTokenData", "current_token_data_topic"),
                ("Token", "token_topic"),
                ("CurrentTokenOwnership", "current_token_ownership_topic"),
                ("CurrentCollectionData", "current_collection_data_topic"),
                ("TokenActivity", "token_activity_topic")
            ]),
        }
    }

    pub fn send<T: Serialize>(&self, model: &str, list_objects: &[T]) {
        let n = list_objects.len();
        let topic = self.get_topic(model);
        for i in 0..n {
            let serialized_obj = serde_json::to_string(&list_objects[i]).unwrap();
            self.producer.send(BaseRecord::<Vec<u8>, _>::to(&topic).payload(serialized_obj.as_bytes())).expect("Failed to send message");
        }
    }

    pub fn send_transaction(&self, model: &str, list_objects: &[Transaction]) {
        let n = list_objects.len();
        let topic = self.get_topic(model);
        for i in 0..n {
            let ic = i.clone();
            match serde_json::to_string(&list_objects[i]) {
                Ok(serialized_obj) => {
                    self.producer.send(BaseRecord::<Vec<u8>, _>::to(&topic).payload(serialized_obj.as_bytes())).expect("Failed to send message");
                }
                Err(err) => {
                    eprintln!("Error serializing object, use another method to serialize");
                    let serialized_obj = &list_objects[ic].to_json_string();
                    let log = serialized_obj.clone();
                    println!("New serialized obj when serializing error: {}", log);
                    self.producer.send(BaseRecord::<Vec<u8>, _>::to(&topic).payload(serialized_obj.as_bytes())).expect("Failed to send message");
                }
            }
        }
    }

    fn get_topic(&self, model: &str) -> &str {
        return &self.topics[self.model_to_topic[model]];
    }
}
