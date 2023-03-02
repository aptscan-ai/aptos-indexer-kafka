// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
    indexer::{
        errors::TransactionProcessingError, processing_result::ProcessingResult,
        transaction_processor::TransactionProcessor,
    },
    models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        events::EventModel,
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{TableItem, TableMetadata},
        signatures::Signature,
        transactions::{TransactionDetail, TransactionModel},
        user_transactions::UserTransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
};
use aptos_api_types::Transaction;
use async_trait::async_trait;
use diesel::{result::Error, PgConnection};
use field_count::FieldCount;
use std::fmt::Debug;
use crate::custom::driver::publisher::Publisher;

pub const NAME: &str = "custom_default_processor";
pub struct CDefaultTransactionProcessor {
    connection_pool: PgDbPool,
    publisher: Publisher,
}

impl CDefaultTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, publisher: Publisher) -> Self {
        Self {
            connection_pool,
            publisher
        }
    }
}

impl Debug for CDefaultTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db(
    publisher: &Publisher,
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: Vec<TransactionModel>,
    txn_details: Vec<TransactionDetail>,
    events: Vec<EventModel>,
    wscs: Vec<WriteSetChangeModel>,
    wsc_details: Vec<WriteSetChangeDetail>,
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
            insert_transactions(publisher, &txns)?;
            insert_user_transactions_w_sigs(publisher, &txn_details)?;
            insert_block_metadata_transactions(publisher, &txn_details)?;
            insert_events(publisher, &events)?;
            insert_write_set_changes(publisher, &wscs)?;
            insert_move_modules(publisher, &wsc_details)?;
            insert_move_resources(publisher, &wsc_details)?;
            insert_table_data(publisher, pg_conn, &wsc_details)?;
            Ok(())
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let txns = clean_data_for_db(txns, true);
                let txn_details = clean_data_for_db(txn_details, true);
                let events = clean_data_for_db(events, true);
                let wscs = clean_data_for_db(wscs, true);
                let wsc_details = clean_data_for_db(wsc_details, true);

                insert_transactions(publisher, &txns)?;
                insert_user_transactions_w_sigs(publisher, &txn_details)?;
                insert_block_metadata_transactions(publisher, &txn_details)?;
                insert_events(publisher, &events)?;
                insert_write_set_changes(publisher, &wscs)?;
                insert_move_modules(publisher, &wsc_details)?;
                insert_move_resources(publisher, &wsc_details)?;
                insert_table_data(publisher, pg_conn, &wsc_details)?;
                Ok(())
            }),
    }
}

fn insert_transactions(
    publisher: &Publisher,
    txns: &[TransactionModel],
) -> Result<(), diesel::result::Error> {
    publisher.send("TransactionModel", txns);
    Ok(())
}

fn insert_user_transactions_w_sigs(
    publisher: &Publisher,
    txn_details: &[TransactionDetail],
) -> Result<(), diesel::result::Error> {
    use schema::{signatures::dsl as sig_schema, user_transactions::dsl as ut_schema};
    let mut all_signatures = vec![];
    let mut all_user_transactions = vec![];
    for detail in txn_details {
        if let TransactionDetail::User(user_txn, sigs) = detail {
            all_signatures.append(&mut sigs.clone());
            all_user_transactions.push(user_txn.clone());
        }
    }
    publisher.send("UserTransactionModel", &all_user_transactions);
    publisher.send("Signature", &all_signatures);
    Ok(())
}

fn insert_block_metadata_transactions(
    publisher: &Publisher,
    txn_details: &[TransactionDetail],
) -> Result<(), diesel::result::Error> {
    use schema::block_metadata_transactions::dsl::*;

    let bmt = txn_details
        .iter()
        .filter_map(|detail| match detail {
            TransactionDetail::BlockMetadata(bmt) => Some(bmt.clone()),
            _ => None,
        })
        .collect::<Vec<BlockMetadataTransactionModel>>();

    publisher.send("BlockMetadataTransactionModel", &bmt);
    Ok(())
}

fn insert_events(publisher: &Publisher, ev: &[EventModel]) -> Result<(), diesel::result::Error> {
    publisher.send("EventModel", ev);
    Ok(())
}

fn insert_write_set_changes(
    publisher: &Publisher,
    wscs: &[WriteSetChangeModel],
) -> Result<(), diesel::result::Error> {
    publisher.send("WriteSetChangeModel", wscs);
    Ok(())
}

fn insert_move_modules(
    publisher: &Publisher,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::move_modules::dsl::*;

    let modules = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Module(module) => Some(module.clone()),
            _ => None,
        })
        .collect::<Vec<MoveModule>>();

    publisher.send("MoveModule", &modules);
    Ok(())
}

fn insert_move_resources(
    publisher: &Publisher,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::move_resources::dsl::*;

    let resources = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Resource(resource) => Some(resource.clone()),
            _ => None,
        })
        .collect::<Vec<MoveResource>>();

    publisher.send("MoveResource", &resources);
    Ok(())
}

/// This will insert all table data within each transaction within a block
fn insert_table_data(
    publisher: &Publisher,
    conn: &mut PgConnection,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::{table_items::dsl as ti, table_metadatas::dsl as tm};

    let (items, metadata): (Vec<TableItem>, Vec<Option<TableMetadata>>) = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Table(table_item, table_metadata) => {
                Some((table_item.clone(), table_metadata.clone()))
            }
            _ => None,
        })
        .collect::<Vec<(TableItem, Option<TableMetadata>)>>()
        .into_iter()
        .unzip();
    let mut metadata_nonnull = metadata
        .iter()
        .filter_map(|x| x.clone())
        .collect::<Vec<TableMetadata>>();
    metadata_nonnull.dedup_by(|a, b| a.handle == b.handle);
    metadata_nonnull.sort_by(|a, b| a.handle.cmp(&b.handle));

    publisher.send("TableItem", &items);

    let chunks = get_chunks(metadata_nonnull.len(), TableMetadata::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::table_metadatas::table)
                .values(&metadata_nonnull[start_ind..end_ind])
                .on_conflict(tm::handle)
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

#[async_trait]
impl TransactionProcessor for CDefaultTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        let (txns, user_txns, bm_txns, events, write_set_changes) =
            TransactionModel::from_transactions(&transactions);

        let mut conn = self.get_conn();
        let tx_result = insert_to_db(
            &self.publisher,
            &mut conn,
            self.name(),
            start_version,
            end_version,
            txns,
            user_txns,
            bm_txns,
            events,
            write_set_changes,
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
