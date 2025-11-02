use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use tonic::{transport::Channel, Request, Response, Status};

pub mod pb {
    tonic::include_proto!("kv");
}
use pb::kv_client::KvClient;
use pb::kv_server::{Kv, KvServer};
use pb::{DelRequest, DelResponse, GetRequest, GetResponse, PutRequest, PutResponse};

#[derive(Clone)]
struct BackendPool {
    clients: Arc<Vec<Arc<RwLock<KvClient<Channel>>>>>,
    rr: Arc<AtomicUsize>,
}

impl BackendPool {
    async fn new(endpoints: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut clients = Vec::with_capacity(endpoints.len());
        for ep in endpoints {
            let ch = Channel::from_shared(format!("http://{ep}"))?
                .connect()
                .await?;
            clients.push(Arc::new(RwLock::new(KvClient::new(ch))));
        }
        Ok(Self {
            clients: Arc::new(clients),
            rr: Arc::new(AtomicUsize::new(0)),
        })
    }

    // round-robin
    fn pick(&self) -> Arc<RwLock<KvClient<Channel>>> {
        let len = self.clients.len().max(1);
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % len;
        self.clients[idx].clone()
    }
}

struct LbSvc {
    pool: BackendPool,
}

#[tonic::async_trait]
impl Kv for LbSvc {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let client = self.pool.pick();
        let mut cli = client.write().await;
        cli.put(Request::new(req))
            .await
            .map(|_| Response::new(PutResponse {}))
            .map_err(map_status)
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let client = self.pool.pick();
        let mut cli = client.write().await;
        match cli.get(Request::new(req)).await {
            Ok(resp) => Ok(resp),
            Err(e) => Err(map_status(e)),
        }
    }

    async fn del(&self, request: Request<DelRequest>) -> Result<Response<DelResponse>, Status> {
        let req = request.into_inner();
        let client = self.pool.pick();
        let mut cli = client.write().await;
        cli.del(Request::new(req))
            .await
            .map(|_| Response::new(DelResponse { removed: true }))
            .map_err(map_status)
    }
}

fn map_status<E: std::fmt::Display>(e: E) -> Status {
    Status::unavailable(format!("backend error: {e}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // LB listens here
    let port: u16 = std::env::var("LB_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60051);
    let bind_ip = std::env::var("LB_BIND_IP").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr: SocketAddr = format!("{bind_ip}:{port}").parse()?;

    // Backends from env CSV (required)
    let peers = std::env::var("PEERS").unwrap_or_default();
    let endpoints: Vec<String> = peers
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if endpoints.is_empty() {
        eprintln!("LB requires PEERS env, csv of host:port backends");
    }
    let pool = BackendPool::new(endpoints).await?;

    println!("zynk-lb listening on {addr}");
    tonic::transport::Server::builder()
        .add_service(KvServer::new(LbSvc { pool }))
        .serve(addr)
        .await?;
    Ok(())
}
