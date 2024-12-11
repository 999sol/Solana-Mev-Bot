use dashmap::DashSet;
use jito_block_engine::block_engine::BlockEnginePackets;
use jito_core::tx_cache::should_forward_tx;
use log::*;
use solana_sdk::transaction::VersionedTransaction;
use std::{
    io,
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::Duration,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    runtime::Runtime,
    select,
    sync::{broadcast::Receiver, mpsc, Mutex},
    time::{interval, sleep, timeout},
};

const HEARTBEAT_LEN: u16 = 4;
const HEARTBEAT_MSG: &[u8; 4] = b"ping";
const HEARTBEAT_MSG_WITH_LENGTH: &[u8; 6] = &[
    (HEARTBEAT_LEN & 0xFF) as u8,
    ((HEARTBEAT_LEN >> 8) & 0xFF) as u8,
    HEARTBEAT_MSG[0],
    HEARTBEAT_MSG[1],
    HEARTBEAT_MSG[2],
    HEARTBEAT_MSG[3],
];

const EXPLORER_ENGINE_URL: &str = ":8374";

#[derive(Error, Debug)]
pub enum ExplorerEngineError {
    #[error("explorer engine failed: {0}")]
    Engine(String),

    #[error("explorer tcp stream failure: {0}")]
    TcpStream(#[from] io::Error),

    #[error("explorer tcp connection timed out")]
    TcpConnectionTimeout(#[from] tokio::time::error::Elapsed),

    #[error("cannot find closest engine")]
    CannotFindEngine(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

pub type ExplorerEngineResult<T> = Result<T, ExplorerEngineError>;

pub struct ExplorerEngineRelayerHandler {
    explorer_engine_forwarder: JoinHandle<()>,
}

pub struct ParsedMessage {
    pub header: [u8; 2],
    pub body: Vec<u8>,
}

pub struct TcpReaderCodec {
    reader: BufReader<OwnedReadHalf>,
}

impl TcpReaderCodec {
    /// Encapsulate a TcpStream with reader functionality
    pub fn new(stream: OwnedReadHalf) -> io::Result<Self> {
        let reader = BufReader::new(stream);
        Ok(Self { reader })
    }

    async fn read_u16_from_bufreader(reader: &mut BufReader<OwnedReadHalf>) -> io::Result<u16> {
        let mut buf = [0; 2];
        reader.read_exact(&mut buf).await?;
        Ok(u16::from_le_bytes(buf))
    }

    async fn read_from_bufreader(reader: &mut BufReader<OwnedReadHalf>) -> io::Result<Vec<u8>> {
        let length = Self::read_u16_from_bufreader(reader).await?;
        let mut buffer = vec![0; length as usize];
        reader.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn read_message(&mut self) -> io::Result<ParsedMessage> {
        let b = Self::read_from_bufreader(&mut self.reader).await?;
        if b.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Message too short",
            ));
        }
        let header = [b[0], b[1]];
        let message = ParsedMessage {
            header,
            body: b[2..].to_vec(),
        };
        Ok(message)
    }
}

impl ExplorerEngineRelayerHandler {
    pub fn new(
        mut explorer_engine_receiver: Receiver<BlockEnginePackets>,
        rpc_servers: Vec<String>,
        restart_interval: Duration,
    ) -> ExplorerEngineRelayerHandler {
        let explorer_engine_forwarder = Builder::new()
            .name("explorer_engine_relayer_handler_thread".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        let result = Self::connect_and_run(
                            &mut explorer_engine_receiver,
                            restart_interval,
                        )
                            .await;

                        if let Err(e) = result {
                            match e {
                                ExplorerEngineError::Engine(_) => {
                                    explorer_engine_receiver = explorer_engine_receiver.resubscribe();
                                    error!("error with explorer engine broadcast receiver, resubscribing to event stream: {:?}", e)
                                },
                                ExplorerEngineError::TcpStream(_) | ExplorerEngineError::TcpConnectionTimeout(_) => {
                                    error!("error with explorer engine connection, attempting to re-establish connection: {:?}", e);
                                },
                                ExplorerEngineError::CannotFindEngine(_) => {
                                    error!("failed to find eligible mempool engine to connect to, retrying: {:?}", e);
                                },
                                ExplorerEngineError::Http(e) => {
                                    error!("failed to connect to mempool engine: {:?}, retrying", e);
                                }
                            }

                            sleep(Duration::from_secs(2)).await;
                        }

                        info!("Restarting ExplorerEngineRelayer");
                        explorer_engine_receiver = explorer_engine_receiver.resubscribe();
                    }
                })
            })
            .unwrap();

        ExplorerEngineRelayerHandler {
            explorer_engine_forwarder,
        }
    }

    async fn connect_and_run(
        explorer_engine_receiver: &mut Receiver<BlockEnginePackets>,
        restart_interval: Duration,
    ) -> ExplorerEngineResult<()> {
        let explorer_engine_url = Self::find_closest_engine().await?;
        info!(
            "determined closest engine in connect and run as {}",
            explorer_engine_url
        );
        let engine_stream = Self::connect_to_engine(&explorer_engine_url).await?;

        let retry_future = sleep(restart_interval);
        tokio::pin!(retry_future);

        select! {
            result = Self::start_event_loop(explorer_engine_receiver, engine_stream) => result,
            _ = &mut retry_future => Ok(()),
        }
    }

    async fn start_event_loop(
        explorer_engine_receiver: &mut Receiver<BlockEnginePackets>,
        explorer_engine_stream: TcpStream,
    ) -> ExplorerEngineResult<()> {
        let (reader, writer) = explorer_engine_stream.into_split();
        let forwarder = Arc::new(Mutex::new(writer));
        let mut heartbeat_interval = interval(Duration::from_secs(5));
        let mut flush_interval = interval(Duration::from_secs(60));
        let tx_cache = Arc::new(DashSet::new());
        let (forward_error_sender, mut forward_error_receiver) = mpsc::unbounded_channel();
       
        loop {
            let cloned_forwarder = forwarder.clone();
            let cloned_error_sender = forward_error_sender.clone();
            let cloned_tx_cache: Arc<DashSet<String>> = tx_cache.clone();

            select! {
                recv_result = explorer_engine_receiver.recv() => {
                    match recv_result {
                        Ok(explorer_engine_batches) => {
                            // Proceed with handling the batches as befores
                            tokio::spawn(async move {
                                for packet_batch in explorer_engine_batches.banking_packet_batch.0.iter() {
                                    for packet in packet_batch {
                                        if packet.meta().discard() || packet.meta().is_simple_vote_tx() {
                                            continue;
                                        }

                                        if let Ok(tx) = packet.deserialize_slice::<VersionedTransaction, _>(..) {
                                            
                                            // build forward msg
                                            let mut tx_data = match bincode::serialize(&tx) {
                                                Ok(data) => data,
                                                Err(_) => continue,
                                            };

                                            let tx_signature = tx.signatures[0].to_string();
                                            if !should_forward_tx(&cloned_tx_cache, &tx_signature) {
                                                continue;
                                            }

                                            let length_bytes = (tx_data.len() as u16).to_le_bytes().to_vec();
                                            tx_data.reserve(2);
                                            tx_data.splice(0..0, length_bytes);

                                            if let Err(e) = Self::forward_packets(cloned_forwarder.clone(), tx_data.as_slice()).await {
                                                if let Err(send_err) = cloned_error_sender.send(e) {
                                                    error!("failed to transmit packet forward error to management channel: {send_err}");
                                                }
                                            } else {
                                                // if send successful, add signature to cache
                                                cloned_tx_cache.insert(tx_signature);
                                                trace!("successfully relayed packets to explorer_engine");
                                            }
                                        }
                                    }
                                };
                            });

                        }
                        Err(e) => match e {
                            tokio::sync::broadcast::error::RecvError::Lagged(n) => {
                                warn!("Receiver lagged by {n} messages, continuing to receive future messages.");
                            }
                            tokio::sync::broadcast::error::RecvError::Closed => {
                                return Err(ExplorerEngineError::Engine("broadcast channel closed".to_string()));
                            }
                        },
                    }
                }
                
                forward_error = forward_error_receiver.recv() => {
                    match forward_error {
                        Some(e) => {
                            return Err(ExplorerEngineError::TcpStream(e))
                        },
                        None => continue,
                    }
                }
                _ = heartbeat_interval.tick() => {
                    info!("sending heartbeat (explorer)");
                    Self::forward_packets(cloned_forwarder.clone(), HEARTBEAT_MSG_WITH_LENGTH).await?;
                }
                _ = flush_interval.tick() => {
                    info!("flushing signature cache");
                    tx_cache.clear();
                }
            }
        }
    }

    pub async fn find_closest_engine() -> ExplorerEngineResult<String> {
        Ok(format!("{}{}", "3.145.46.242", EXPLORER_ENGINE_URL))
    }

    pub async fn connect_to_engine(engine_url: &str) -> ExplorerEngineResult<TcpStream> {
        let stream_future = TcpStream::connect(engine_url);

        let stream = timeout(Duration::from_secs(10), stream_future).await??;

        if let Err(e) = stream.set_nodelay(true) {
            warn!(
                "TcpStream NAGLE disable failed ({e:?}) - packet delivery will be slightly delayed"
            )
        }

        info!("successfully connected to explorer tcp engine!");

        Ok(stream)
    }

    pub async fn forward_packets(
        stream: Arc<Mutex<OwnedWriteHalf>>,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        stream.lock().await.write_all(data).await
    }

    pub fn join(self) {
        self.explorer_engine_forwarder.join().unwrap();
    }
}
