//! Dispatch of every request to the driver.

use std::path::PathBuf;
use std::time::Duration;

use bson::{doc, Bson, Document, RawArrayBuf, RawDocumentBuf};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{
    AggregateOptions, BulkWriteOptions, ChangeStreamOptions, ClientOptions, CountOptions, DeleteManyModel,
    DeleteOneModel, DeleteOptions, FindOneAndDeleteOptions, FindOneAndUpdateOptions, FindOneOptions, FindOptions, Hint,
    IndexOptions, InsertManyOptions, InsertOneModel, InsertOneOptions, ReplaceOneModel, ReplaceOptions, ReturnDocument,
    Tls, TlsOptions, UpdateManyModel, UpdateOneModel, UpdateOptions, WriteModel,
};
use mongodb::{Client, Collection, IndexModel, Namespace};

use crate::cursor;
use crate::error::{from_driver, NativeError};
use crate::registry;
use crate::reply;
use crate::request::Request;
use crate::stream::{self, StreamState, Watch};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether an interrupted [op] (client timeout, cancellation, close) may have been applied.
pub fn outcome_unknown(op: &str, req: &Request) -> bool {
    match op {
        "insertOne" | "insertMany" | "updateOne" | "updateMany" | "replaceOne" | "deleteOne" | "deleteMany"
        | "bulkWrite" | "findOneAndUpdate" | "findOneAndDelete" | "createIndexes" | "dropIndex" | "dropCollection" => {
            true
        }
        "runCommand" => !req.bool_or("readOnly", false),
        _ => false,
    }
}

/// Decodes [bytes], runs the request under its client clock and returns the reply document.
pub async fn dispatch(request: u64, bytes: Vec<u8>) -> Result<RawDocumentBuf, NativeError> {
    let req = Request::parse(&bytes)?;
    let target = req.target();
    let unknown = outcome_unknown(&req.op, &req);
    let client = match req.op.as_str() {
        "client.open" | "client.close" | "cursor.close" | "stream.close" => None,
        _ => Some(req.handle("client")?),
    };
    registry::describe(request, &target, unknown, client)?;
    let timeout = req.millis("timeoutMs").unwrap_or(DEFAULT_TIMEOUT);
    match tokio::time::timeout(timeout, Box::pin(run(&req, &target))).await {
        Ok(result) => result,
        Err(_) => Err(NativeError::new(
            "timeout",
            &target,
            format!(
                "no answer within {} ms{}",
                timeout.as_millis(),
                if unknown {
                    "; the server may have applied the write"
                } else {
                    ""
                }
            ),
        )
        .with("outcomeUnknown", unknown)),
    }
}

async fn run(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let op = req.op.as_str();
    match op {
        "client.open" => open_client(req, target).await,
        "client.close" => close_client(req, target).await,
        "cursor.next" => cursor::next(req, target).await,
        "cursor.close" => cursor::close(req, target).await,
        "stream.next" => stream::next(req, target).await,
        "stream.close" => stream::close(req, target).await,
        "bulkWrite" => bulk_write(req, target).await,
        "runCommand" => run_command(req, target).await,
        "listCollectionNames" => list_collection_names(req, target).await,
        "dropCollection" => drop_collection(req, target).await,
        "watch" => watch(req, target).await,
        _ => collection_op(req, target).await,
    }
}

fn driver(target: &str) -> impl FnOnce(mongodb::error::Error) -> NativeError + '_ {
    move |e| from_driver(target, e)
}

async fn open_client(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let uri = req.str("uri")?;
    let mut options = ClientOptions::parse(uri).await.map_err(driver(target))?;
    if let Some(name) = req.opt_str("appName") {
        options.app_name = Some(name.to_string());
    }
    if let Some(n) = req.opt_int("maxPoolSize") {
        options.max_pool_size = Some(n as u32);
    }
    if let Some(n) = req.opt_int("minPoolSize") {
        options.min_pool_size = Some(n as u32);
    }
    if let Some(d) = req.millis("maxIdleTimeMs") {
        options.max_idle_time = Some(d);
    }
    if let Some(d) = req.millis("connectTimeoutMs") {
        options.connect_timeout = Some(d);
    }
    if let Some(d) = req.millis("serverSelectionTimeoutMs") {
        options.server_selection_timeout = Some(d);
    }
    if let Some(d) = req.millis("heartbeatFrequencyMs") {
        options.heartbeat_freq = Some(d);
    }
    if let Some(tls) = req.opt_doc("tls") {
        let mut t = TlsOptions::default();
        t.ca_file_path = tls.get_str("caFile").ok().map(PathBuf::from);
        t.cert_key_file_path = tls.get_str("certificateKeyFile").ok().map(PathBuf::from);
        options.tls = Some(Tls::Enabled(t));
    }
    if let Some(wc) = req.write_concern() {
        options.write_concern = Some(wc);
    }
    if let Some(rc) = req.read_concern() {
        options.read_concern = Some(rc);
    }
    if let Some(b) = req.opt_bool("retryWrites") {
        options.retry_writes = Some(b);
    }
    if let Some(b) = req.opt_bool("retryReads") {
        options.retry_reads = Some(b);
    }
    let client = Client::with_options(options).map_err(driver(target))?;
    let handle = registry::insert_client(client);
    reply::document(target, &doc! { "client": handle as i64 })
}

async fn close_client(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("client")?;
    let closing = registry::close_client(handle, target)?;
    drop(closing.cursors);
    drop(closing.streams);
    registry::settled(handle).await;
    closing.client.shutdown().immediate(true).await;
    registry::remove_client(handle);
    reply::document(target, &doc! {})
}

fn namespace_of(model: &Document, target: &str) -> Result<Namespace, NativeError> {
    match (model.get_str("db"), model.get_str("coll")) {
        (Ok(db), Ok(coll)) => Ok(Namespace::new(db, coll)),
        _ => Err(NativeError::bson(target, "a write model has no string `db` and `coll`")),
    }
}

fn model_doc(model: &Document, key: &str, target: &str) -> Result<Document, NativeError> {
    model
        .get_document(key)
        .cloned()
        .map_err(|_| NativeError::bson(target, format!("a write model has no document `{key}`")))
}

fn model_update(model: &Document, target: &str) -> Result<mongodb::options::UpdateModifications, NativeError> {
    match model.get("update") {
        Some(Bson::Document(d)) => Ok(mongodb::options::UpdateModifications::Document(d.clone())),
        Some(Bson::Array(items)) => items
            .iter()
            .map(|i| match i {
                Bson::Document(d) => Ok(d.clone()),
                _ => Err(NativeError::bson(
                    target,
                    "an update pipeline holds a value that is not a document",
                )),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(mongodb::options::UpdateModifications::Pipeline),
        _ => Err(NativeError::bson(
            target,
            "a write model has no update document or pipeline",
        )),
    }
}

fn write_model(model: &Document, target: &str) -> Result<WriteModel, NativeError> {
    let ns = namespace_of(model, target)?;
    let upsert = model.get_bool("upsert").unwrap_or(false);
    let kind = model.get_str("type").unwrap_or_default();
    Ok(match kind {
        "insertOne" => InsertOneModel::builder()
            .namespace(ns)
            .document(model_doc(model, "document", target)?)
            .build()
            .into(),
        "updateOne" => UpdateOneModel::builder()
            .namespace(ns)
            .filter(model_doc(model, "filter", target)?)
            .update(model_update(model, target)?)
            .upsert(upsert)
            .build()
            .into(),
        "updateMany" => UpdateManyModel::builder()
            .namespace(ns)
            .filter(model_doc(model, "filter", target)?)
            .update(model_update(model, target)?)
            .upsert(upsert)
            .build()
            .into(),
        "replaceOne" => ReplaceOneModel::builder()
            .namespace(ns)
            .filter(model_doc(model, "filter", target)?)
            .replacement(model_doc(model, "replacement", target)?)
            .upsert(upsert)
            .build()
            .into(),
        "deleteOne" => DeleteOneModel::builder()
            .namespace(ns)
            .filter(model_doc(model, "filter", target)?)
            .build()
            .into(),
        "deleteMany" => DeleteManyModel::builder()
            .namespace(ns)
            .filter(model_doc(model, "filter", target)?)
            .build()
            .into(),
        other => return Err(NativeError::bson(target, format!("unknown write model type `{other}`"))),
    })
}

async fn bulk_write(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let client = registry::client(req.handle("client")?, target)?;
    let models = req
        .docs("models")?
        .iter()
        .map(|m| write_model(m, target))
        .collect::<Result<Vec<_>, _>>()?;
    let mut options = BulkWriteOptions::default();
    options.ordered = Some(req.bool_or("ordered", true));
    options.write_concern = req.write_concern();
    let result = client
        .bulk_write(models)
        .verbose_results()
        .with_options(options)
        .await
        .map_err(driver(target))?;
    let mut ids: Vec<(usize, Bson)> = result
        .update_results
        .iter()
        .filter_map(|(i, r)| r.upserted_id.clone().map(|id| (*i, id)))
        .collect();
    ids.sort_by_key(|(i, _)| *i);
    let summary = doc! {
        "inserted": result.summary.inserted_count, "matched": result.summary.matched_count,
        "modified": result.summary.modified_count, "deleted": result.summary.deleted_count,
        "upserted": result.summary.upserted_count,
        "upsertedIds": ids.into_iter().map(|(i, id)| Bson::Document(doc! { "index": i as i64, "id": id })).collect::<Vec<_>>(),
    };
    reply::document(target, &summary)
}

async fn run_command(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let client = registry::client(req.handle("client")?, target)?;
    let db = client.database(req.str("db")?);
    let result = db.run_command(req.doc("command")?).await.map_err(driver(target))?;
    reply::document(target, &doc! { "result": result })
}

async fn list_collection_names(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let client = registry::client(req.handle("client")?, target)?;
    let names = client
        .database(req.str("db")?)
        .list_collection_names()
        .await
        .map_err(driver(target))?;
    reply::document(target, &doc! { "names": names })
}

async fn drop_collection(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let client = registry::client(req.handle("client")?, target)?;
    client
        .database(req.str("db")?)
        .collection::<Document>(req.str("coll")?)
        .drop()
        .await
        .map_err(driver(target))?;
    reply::document(target, &doc! {})
}

async fn watch(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("client")?;
    let client = registry::client(handle, target)?;
    let max_await = req.millis("maxAwaitMs").unwrap_or(Duration::from_millis(500));
    let batch_size = req.opt_int("batchSize").unwrap_or(1000).max(1);
    let mut options = ChangeStreamOptions::default();
    options.max_await_time = Some(max_await);
    options.batch_size = Some(batch_size as u32);
    options.start_at_operation_time = req.opt_timestamp("startAtOperationTime");
    if let Some(token) = req.opt_doc("resumeAfter") {
        let token: ResumeToken = bson::from_bson(Bson::Document(token))
            .map_err(|e| NativeError::bson(target, format!("resumeAfter is not a resume token: {e}")))?;
        options.resume_after = Some(token);
    }
    let watch = Watch {
        client,
        db: req.str("db")?.to_string(),
        coll: req.opt_str("coll").map(str::to_string),
        pipeline: req.docs("pipeline").unwrap_or_default(),
        options,
    };
    let stream = watch.open(None).await.map_err(driver(target))?;
    let state = StreamState::start(watch, stream, max_await, batch_size as usize, target.to_string());
    stream::opened(handle, state, target).await
}

fn hint(req: &Request) -> Option<Hint> {
    req.opt_str("hint").map(|h| Hint::Name(h.to_string()))
}

fn raw_or_null(target: &str, doc: Option<RawDocumentBuf>) -> Result<RawDocumentBuf, NativeError> {
    let mut out = reply::document(target, &doc! {})?;
    match doc {
        Some(d) => out.append("document", d),
        None => out.append("document", bson::RawBson::Null),
    }
    Ok(out)
}

async fn collection_op(req: &Request, target: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("client")?;
    let client = registry::client(handle, target)?;
    let coll: Collection<Document> = client.database(req.str("db")?).collection(req.str("coll")?);
    let raw: Collection<RawDocumentBuf> = coll.clone_with_type();
    let max_time = req.millis("maxTimeMs");
    match req.op.as_str() {
        "insertOne" => {
            let mut o = InsertOneOptions::default();
            o.write_concern = req.write_concern();
            let r = coll
                .insert_one(req.doc("document")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            let mut out = RawDocumentBuf::new();
            out.append("insertedId", reply::raw(target, r.inserted_id)?);
            Ok(out)
        }
        "insertMany" => {
            let mut o = InsertManyOptions::default();
            o.write_concern = req.write_concern();
            o.ordered = Some(req.bool_or("ordered", true));
            let r = coll
                .insert_many(req.docs("documents")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            let mut ids: Vec<(usize, Bson)> = r.inserted_ids.into_iter().collect();
            ids.sort_by_key(|(i, _)| *i);
            reply::document(
                target,
                &doc! { "insertedIds": ids.into_iter().map(|(_, id)| id).collect::<Vec<_>>() },
            )
        }
        "updateOne" | "updateMany" => {
            let mut o = UpdateOptions::default();
            o.write_concern = req.write_concern();
            o.upsert = Some(req.bool_or("upsert", false));
            let filter = req.doc("filter")?;
            let update = req.update("update")?;
            let r = if req.op == "updateOne" {
                coll.update_one(filter, update).with_options(o).await
            } else {
                coll.update_many(filter, update).with_options(o).await
            }
            .map_err(driver(target))?;
            reply::document(
                target,
                &doc! {
                    "matched": r.matched_count as i64, "modified": r.modified_count as i64,
                    "upsertedId": r.upserted_id.unwrap_or(Bson::Null),
                },
            )
        }
        "replaceOne" => {
            let mut o = ReplaceOptions::default();
            o.write_concern = req.write_concern();
            o.upsert = Some(req.bool_or("upsert", false));
            let r = coll
                .replace_one(req.doc("filter")?, req.doc("replacement")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            reply::document(
                target,
                &doc! {
                    "matched": r.matched_count as i64, "modified": r.modified_count as i64,
                    "upsertedId": r.upserted_id.unwrap_or(Bson::Null),
                },
            )
        }
        "deleteOne" | "deleteMany" => {
            let mut o = DeleteOptions::default();
            o.write_concern = req.write_concern();
            let filter = req.doc("filter")?;
            let r = if req.op == "deleteOne" {
                coll.delete_one(filter).with_options(o).await
            } else {
                coll.delete_many(filter).with_options(o).await
            }
            .map_err(driver(target))?;
            reply::document(target, &doc! { "deleted": r.deleted_count as i64 })
        }
        "findOne" => {
            let mut o = FindOneOptions::default();
            o.projection = req.opt_doc("projection");
            o.sort = req.opt_doc("sort");
            o.max_time = max_time;
            o.read_concern = req.read_concern();
            let d = raw
                .find_one(req.doc("filter")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            raw_or_null(target, d)
        }
        "find" => {
            let batch_size = req.opt_int("batchSize").unwrap_or(1000).max(1);
            let mut o = FindOptions::default();
            o.projection = req.opt_doc("projection");
            o.sort = req.opt_doc("sort");
            o.skip = req.opt_int("skip").filter(|s| *s > 0).map(|s| s as u64);
            o.limit = req.opt_int("limit");
            o.batch_size = Some(batch_size as u32);
            o.hint = hint(req);
            o.max_time = max_time;
            o.read_concern = req.read_concern();
            let c = raw
                .find(req.doc("filter")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            cursor::open(handle, c, batch_size as usize, target).await
        }
        "aggregate" => {
            let batch_size = req.opt_int("batchSize").unwrap_or(1000).max(1);
            let mut o = AggregateOptions::default();
            o.batch_size = Some(batch_size as u32);
            o.max_time = max_time;
            o.read_concern = req.read_concern();
            let c = coll
                .aggregate(req.docs("pipeline")?)
                .with_options(o)
                .await
                .map_err(driver(target))?
                .with_type::<RawDocumentBuf>();
            cursor::open(handle, c, batch_size as usize, target).await
        }
        "findOneAndUpdate" => {
            let mut o = FindOneAndUpdateOptions::default();
            o.upsert = Some(req.bool_or("upsert", false));
            o.return_document = Some(if req.bool_or("returnAfter", true) {
                ReturnDocument::After
            } else {
                ReturnDocument::Before
            });
            o.sort = req.opt_doc("sort");
            o.projection = req.opt_doc("projection");
            o.max_time = max_time;
            o.write_concern = req.write_concern();
            let d = raw
                .find_one_and_update(req.doc("filter")?, req.update("update")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            raw_or_null(target, d)
        }
        "findOneAndDelete" => {
            let mut o = FindOneAndDeleteOptions::default();
            o.sort = req.opt_doc("sort");
            o.projection = req.opt_doc("projection");
            o.max_time = max_time;
            o.write_concern = req.write_concern();
            let d = raw
                .find_one_and_delete(req.doc("filter")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            raw_or_null(target, d)
        }
        "countDocuments" => {
            let mut o = CountOptions::default();
            o.hint = hint(req);
            o.max_time = max_time;
            o.read_concern = req.read_concern();
            let n = coll
                .count_documents(req.doc("filter")?)
                .with_options(o)
                .await
                .map_err(driver(target))?;
            reply::document(target, &doc! { "count": n as i64 })
        }
        "createIndexes" => {
            let models = req
                .docs("indexes")?
                .into_iter()
                .map(|spec| {
                    let keys = spec
                        .get_document("keys")
                        .cloned()
                        .map_err(|_| NativeError::bson(target, "an index has no document `keys`"))?;
                    let mut o = IndexOptions::default();
                    o.name = spec.get_str("name").ok().map(str::to_string);
                    o.unique = spec.get_bool("unique").ok();
                    o.expire_after = match spec.get("expireAfterSeconds") {
                        Some(Bson::Int32(s)) => Some(Duration::from_secs(*s as u64)),
                        Some(Bson::Int64(s)) => Some(Duration::from_secs(*s as u64)),
                        _ => None,
                    };
                    o.partial_filter_expression = spec.get_document("partialFilter").ok().cloned();
                    Ok(IndexModel::builder().keys(keys).options(o).build())
                })
                .collect::<Result<Vec<_>, NativeError>>()?;
            let r = coll.create_indexes(models).await.map_err(driver(target))?;
            reply::document(target, &doc! { "names": r.index_names })
        }
        "dropIndex" => {
            coll.drop_index(req.str("name")?).await.map_err(driver(target))?;
            reply::document(target, &doc! {})
        }
        "listIndexes" => {
            let mut c = coll
                .list_indexes()
                .await
                .map_err(driver(target))?
                .with_type::<RawDocumentBuf>();
            let mut all = RawArrayBuf::new();
            while c.advance().await.map_err(driver(target))? {
                all.push(c.current().to_owned());
            }
            let mut out = RawDocumentBuf::new();
            out.append("indexes", all);
            Ok(out)
        }
        other => Err(NativeError::bson(target, format!("unknown operation `{other}`"))),
    }
}
