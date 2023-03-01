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
            publisher,
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
) -> Result<(), diesel::result::Error> {
    // store coin info in db
    insert_coin_infos(conn, coin_infos)?;

    // the others send to kafka
    insert_coin_activities(publisher, coin_activities)?;
    insert_coin_balances(publisher, coin_balances)?;
    insert_current_coin_balances(publisher, current_coin_balances)?;
    insert_coin_supply(publisher, coin_supply)?;
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

                insert_to_db_impl(
                    publisher,
                    pg_conn,
                    &coin_activities,
                    &coin_infos,
                    &coin_balances,
                    &current_coin_balances,
                    &coin_supply,
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
    conn: &mut PgConnection,
    item_to_insert: &[CoinInfo],
) -> Result<(), diesel::result::Error> {
    use schema::coin_infos::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinInfo::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_infos::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict(coin_type_hash)
                .do_update()
                .set((
                    transaction_version_created.eq(excluded(transaction_version_created)),
                    creator_address.eq(excluded(creator_address)),
                    name.eq(excluded(name)),
                    symbol.eq(excluded(symbol)),
                    decimals.eq(excluded(decimals)),
                    transaction_created_timestamp.eq(excluded(transaction_created_timestamp)),
                    supply_aggregator_table_handle.eq(excluded(supply_aggregator_table_handle)),
                    supply_aggregator_table_key.eq(excluded(supply_aggregator_table_key)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE coin_infos.transaction_version_created >= EXCLUDED.transaction_version_created "),
        )?;
    }
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
        }
        let mut all_coin_infos = all_coin_infos.into_values().collect::<Vec<CoinInfo>>();
        let mut all_current_coin_balances = all_current_coin_balances
            .into_values()
            .collect::<Vec<CurrentCoinBalance>>();

        // Sort by PK
        all_coin_infos.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
        all_current_coin_balances.sort_by(|a, b| {
            (&a.owner_address, &a.coin_type).cmp(&(&b.owner_address, &b.coin_type))
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
