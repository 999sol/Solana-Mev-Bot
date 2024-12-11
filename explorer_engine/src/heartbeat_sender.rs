use std::error::Error;
use std::sync::Arc;
use log::warn;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task;

pub struct OnChainHeartbeatSender {
    urls: Vec<String>,
    sender: mpsc::Sender<(String, Vec<u8>)>,
}

impl OnChainHeartbeatSender {
    pub fn new(urls: Vec<String>) -> Self {
        let (sender, mut receiver) = mpsc::channel(128);
        let client = Arc::new(Client::new());

        let client_clone = Arc::clone(&client);
        task::spawn(async move {
            while let Some((url, payload)) = receiver.recv().await {
                let client_clone = Arc::clone(&client_clone);
                task::spawn(async move {
                    if let Err(e) = send_post_request(&client_clone, &url, payload).await {
                        warn!("Failed to send request to {}: {}", url, e);
                    }
                });
            }
        });

        Self { urls, sender }
    }

    pub async fn broadcast(&self, payload: Vec<u8>) -> Result<(), Box<dyn Error>> {
        for url in &self.urls {
            self.sender.send((url.clone(), payload.clone())).await?;
        }
        Ok(())
    }
}

async fn send_post_request(client: &Client, url: &String, payload: Vec<u8>) -> Result<(), reqwest::Error> {
    let _ = client
        .post(url)
        .body(payload.clone())
        .header("Content-Type", "application/json")
        .send()
        .await?;
    Ok(())
}