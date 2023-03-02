use std::collections::HashMap;
use serde::Serialize;

use {
    rdkafka::{
        producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    },
};

use crate::custom::driver::config::DriverConfig;
use crate::custom::driver::producer::Producer;

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
                // transaction
                ("TransactionModel", "transaction_topic"),
                ("UserTransactionModel", "user_transaction_topic"),
                ("Signature", "signature_topic"),
                ("BlockMetadataTransactionModel", "block_metadata_transaction_topic"),
                ("EventModel", "event_topic"),
                ("WriteSetChangeModel", "write_set_change_topic"),
                ("MoveResource", "move_resource_topic"),
                ("TableItem", "table_item_topic"),
                ("CurrentTableItem", "current_table_item_topic"),
                ("MoveModule", "move_module_topic"),
                // coin
                ("CoinActivity", "coin_activity_topic"),
                ("CoinBalance", "coin_balance_topic"),
                ("CurrentCoinBalance", "current_coin_balance_topic"),
                ("CoinSupply", "coin_supply_topic"),
                // token
                ("TokenActivity", "token_activity_topic"),
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

    fn get_topic(&self, model: &str) -> &str {
        return &self.topics[self.model_to_topic[model]];
    }
}
