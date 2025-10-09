use crate::{
    node::identifier::Key,
    node::storage::Value,
};

use crate::protocol::{ProtocolManager, Command};

use tokio::net::UdpSocket;
use tokio::{sync::{mpsc, oneshot}};

pub struct KademliaDHT {
    tx: mpsc::Sender<Command>,
}


impl KademliaDHT {
    pub async fn start() -> anyhow::Result<Self> {
	let (tx, rx) = mpsc::channel::<Command>(100);
	let socket = UdpSocket::bind("0.0.0.0:8080").await?;
	let k = 20;
	let alpha = 3;
	let manager = ProtocolManager::new(socket, rx, k, alpha)?;

	tokio::spawn(
	    manager.run()
	);

	Ok(Self{tx})

    }

    pub async fn put(&self, key: Key, value: Value) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Put { key, value,  response: tx }).await?;
        rx.await?
    }

    pub async fn get(&self, key: Key) -> anyhow::Result<(Option<Value>)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Get { key, response: tx }).await?;
        rx.await?
    }

}
