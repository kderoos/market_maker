use tokio::time::Duration;

/// Delay all messages on a broadcast channel by `interval_ms` milliseconds.
/// Messages are received, delayed, and re-sent to a new channel.
pub async fn chan_delayer<T: Clone + Send + 'static>(
    mut rx: Receiver<T>,
     tx: Sender<T>,
    interval_ms: u64,
) {
    let delay = Duration::from_millis(interval_ms);
    loop {
        if let Ok(msg) = rx.recv().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(msg);
            });
        }
    }
}

