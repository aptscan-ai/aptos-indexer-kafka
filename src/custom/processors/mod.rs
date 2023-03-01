pub mod custom_coin_processor;
pub mod custom_default_processor;
pub mod custom_token_processor;
pub mod custom_stake_processor;


use self::{
    custom_coin_processor::NAME as COIN_PROCESSOR_NAME, custom_default_processor::NAME as DEFAULT_PROCESSOR_NAME,
    custom_stake_processor::NAME as STAKE_PROCESSOR_NAME, custom_token_processor::NAME as TOKEN_PROCESSOR_NAME
};

pub enum CProcessor {
    CoinProcessor,
    DefaultProcessor,
    TokenProcessor,
    StakeProcessor
}

impl CProcessor {
    pub fn from_string(input_str: &String) -> Self {
        match input_str.as_str() {
            DEFAULT_PROCESSOR_NAME => Self::DefaultProcessor,
            TOKEN_PROCESSOR_NAME => Self::TokenProcessor,
            COIN_PROCESSOR_NAME => Self::CoinProcessor,
            STAKE_PROCESSOR_NAME => Self::StakeProcessor,
            _ => panic!("Processor unsupported {}", input_str),
        }
    }
}
