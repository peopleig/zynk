use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use zynk::engine::kv::LsmEngine;

pub mod pb {
    tonic::include_proto!("kv");
}

use pb::kv_server::{Kv, KvServer};
use pb::{DelRequest, DelResponse, GetRequest, GetResponse, PutRequest, PutResponse};

struct KvSvc {
    engine: Arc<RwLock<LsmEngine>>,
}

#[tonic::async_trait]
impl Kv for KvSvc {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let mut eng = self.engine.write().await;
        eng.put(&req.key, &req.value).map_err(to_status)?;
        Ok(Response::new(PutResponse {}))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let eng = self.engine.read().await;
        match eng.get(&req.key).map_err(to_status)? {
            Some(v) => Ok(Response::new(GetResponse {
                value: v,
                found: true,
            })),
            None => Ok(Response::new(GetResponse {
                value: Vec::new(),
                found: false,
            })),
        }
    }

    async fn del(&self, request: Request<DelRequest>) -> Result<Response<DelResponse>, Status> {
        let req = request.into_inner();
        let mut eng = self.engine.write().await;
        eng.delete(&req.key).map_err(to_status)?;
        Ok(Response::new(DelResponse { removed: true }))
    }
}

fn get_or_create_actor_id(data_dir: &PathBuf) -> std::io::Result<u64> {
    let id_path = data_dir.join("actor_id");
    if id_path.exists() {
        let s = fs::read_to_string(&id_path)?;
        if let Ok(id) = s.trim().parse::<u64>() {
            return Ok(id);
        }
    }
    use rand::{thread_rng, Rng};
    let id: u64 = thread_rng().gen();
    fs::write(&id_path, id.to_string())?;
    Ok(id)
}

fn to_status(e: std::io::Error) -> Status {
    Status::internal(e.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50051);
    let bind_ip = std::env::var("BIND_IP").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr: SocketAddr = format!("{bind_ip}:{port}").parse()?;

    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string()));
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "node-unknown".to_string());

    // derive actor id for this node:
    let actor_id = get_or_create_actor_id(&data_dir)?;

    let engine = LsmEngine::new_with_manifest_and_actor(&data_dir, 64 * 1024, 8 * 1024, actor_id)?;
    let svc = KvSvc {
        engine: Arc::new(RwLock::new(engine)),
    };

    println!(
        "zynkd listening on {} (ACTOR_ID={}, DATA_DIR={})",
        addr,
        actor_id,
        data_dir.display()
    );
    tonic::transport::Server::builder()
        .add_service(KvServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
