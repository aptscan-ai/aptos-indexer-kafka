// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
    indexer::{
        errors::TransactionProcessingError, processing_result::ProcessingResult,
        transaction_processor::TransactionProcessor,
    },
    models::coin_models::{
        account_transactions::AccountTransaction,
        coin_activities::{CoinActivity, CurrentCoinBalancePK},
        coin_balances::{CoinBalance, CurrentCoinBalance},
        coin_infos::{CoinInfo, CoinInfoQuery},
        coin_supply::CoinSupply,
    },
    schema,
};
use aptos_api_types::Transaction as APITransaction;
use aptos_types::APTOS_COIN_TYPE;
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use crate::custom::driver::publisher::Publisher;

pub const NAME: &str = "custom_coin_processor";
pub struct CCoinTransactionProcessor {
    connection_pool: PgDbPool,
    publisher: Publisher,
}

impl CCoinTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, publisher: Publisher) -> Self {
        Self {
            connection_pool,
            publisher
        }
    }
}

impl Debug for CCoinTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "CoinTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db_impl(
    publisher: &Publisher,
    conn: &mut PgConnection,
    coin_activities: &[CoinActivity],
    coin_infos: &[CoinInfo],
    coin_balances: &[CoinBalance],
    current_coin_balances: &[CurrentCoinBalance],
    coin_supply: &[CoinSupply],
    account_transactions: &[AccountTransaction],
) -> Result<(), diesel::result::Error> {
    // insert_coin_activities(publisher, coin_activities)?;
    // insert_coin_infos(publisher, coin_infos)?;
    // insert_coin_balances(publisher, coin_balances)?;
    // insert_current_coin_balances(publisher, current_coin_balances)?;
    // insert_coin_supply(publisher, coin_supply)?;
    // insert_account_transactions(conn, account_transactions)?;
    Ok(())
}

fn insert_to_db(
    publisher: &Publisher,
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    coin_activities: Vec<CoinActivity>,
    coin_infos: Vec<CoinInfo>,
    coin_balances: Vec<CoinBalance>,
    current_coin_balances: Vec<CurrentCoinBalance>,
    coin_supply: Vec<CoinSupply>,
    account_transactions: Vec<AccountTransaction>,
) -> Result<(), diesel::result::Error> {
    aptos_logger::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            insert_to_db_impl(
                publisher,
                pg_conn,
                &coin_activities,
                &coin_infos,
                &coin_balances,
                &current_coin_balances,
                &coin_supply,
                &account_transactions,
            )
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let coin_activities = clean_data_for_db(coin_activities, true);
                let coin_infos = clean_data_for_db(coin_infos, true);
                let coin_balances = clean_data_for_db(coin_balances, true);
                let current_coin_balances = clean_data_for_db(current_coin_balances, true);
                let coin_supply = clean_data_for_db(coin_supply, true);
                let account_transactions = clean_data_for_db(account_transactions, true);

                insert_to_db_impl(
                    publisher,
                    pg_conn,
                    &coin_activities,
                    &coin_infos,
                    &coin_balances,
                    &current_coin_balances,
                    &coin_supply,
                    &account_transactions,
                )
            }),
    }
}

fn insert_coin_activities(
    publisher: &Publisher,
    item_to_insert: &[CoinActivity],
) -> Result<(), diesel::result::Error> {
    publisher.send("CoinActivity", item_to_insert);
    Ok(())
}

fn insert_coin_infos(
    publisher: &Publisher,
    item_to_insert: &[CoinInfo],
) -> Result<(), diesel::result::Error> {
    publisher.send("CoinInfo", item_to_insert);
    Ok(())
}

fn insert_coin_balances(
    publisher: &Publisher,
    item_to_insert: &[CoinBalance],
) -> Result<(), diesel::result::Error> {
    publisher.send("CoinBalance", item_to_insert);
    Ok(())
}

fn insert_current_coin_balances(
    publisher: &Publisher,
    item_to_insert: &[CurrentCoinBalance],
) -> Result<(), diesel::result::Error> {
    publisher.send("CurrentCoinBalance", item_to_insert);
    Ok(())
}

fn insert_coin_supply(
    publisher: &Publisher,
    item_to_insert: &[CoinSupply],
) -> Result<(), diesel::result::Error> {
    publisher.send("CoinSupply", item_to_insert);
    Ok(())
}

#[async_trait]
impl TransactionProcessor for CCoinTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<APITransaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        let mut conn = self.get_conn();
        // get aptos_coin info for supply tracking
        // TODO: This only needs to be fetched once. Need to persist somehow
        let maybe_aptos_coin_info =
            &CoinInfoQuery::get_by_coin_type(APTOS_COIN_TYPE.to_string(), &mut conn).unwrap();

        let mut all_coin_activities = vec![];
        let mut all_coin_balances = vec![];
        let mut all_coin_infos: HashMap<String, CoinInfo> = HashMap::new();
        let mut all_current_coin_balances: HashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
            HashMap::new();
        let mut all_coin_supply = vec![];

        let mut account_transactions = HashMap::new();

        for txn in &transactions {
            let (
                mut coin_activities,
                mut coin_balances,
                coin_infos,
                current_coin_balances,
                mut coin_supply,
            ) = CoinActivity::from_transaction(txn, maybe_aptos_coin_info);
            all_coin_activities.append(&mut coin_activities);
            all_coin_balances.append(&mut coin_balances);
            all_coin_supply.append(&mut coin_supply);
            // For coin infos, we only want to keep the first version, so insert only if key is not present already
            for (key, value) in coin_infos {
                all_coin_infos.entry(key).or_insert(value);
            }
            all_current_coin_balances.extend(current_coin_balances);

            account_transactions.extend(AccountTransaction::from_transaction(txn).unwrap());
        }
        let mut all_coin_infos = all_coin_infos.into_values().collect::<Vec<CoinInfo>>();
        let mut all_current_coin_balances = all_current_coin_balances
            .into_values()
            .collect::<Vec<CurrentCoinBalance>>();
        let mut account_transactions = account_transactions
            .into_values()
            .collect::<Vec<AccountTransaction>>();

        // Sort by PK
        all_coin_infos.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
        all_current_coin_balances.sort_by(|a, b| {
            (&a.owner_address, &a.coin_type).cmp(&(&b.owner_address, &b.coin_type))
        });
        account_transactions.sort_by(|a, b| {
            (&a.transaction_version, &a.account_address)
                .cmp(&(&b.transaction_version, &b.account_address))
        });

        let tx_result = insert_to_db(
            &self.publisher,
            &mut conn,
            self.name(),
            start_version,
            end_version,
            all_coin_activities,
            all_coin_infos,
            all_coin_balances,
            all_current_coin_balances,
            all_coin_supply,
            account_transactions,
        );
        match tx_result {
            Ok(_) => Ok(ProcessingResult::new(
                self.name(),
                start_version,
                end_version,
            )),
            Err(err) => Err(TransactionProcessingError::TransactionCommitError((
                anyhow::Error::from(err),
                start_version,
                end_version,
                self.name(),
            ))),
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
