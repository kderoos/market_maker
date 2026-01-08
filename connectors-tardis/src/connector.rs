use crate::cursor::merge::MergeCursor;
use crate::normalize::normalize;
use async_trait::async_trait;

pub struct TardisConnector {
    cursor: MergeCursor,
}

#[async_trait]
impl Connector for TardisConnector {
    fn name(&self) -> &'static str {
        "tardis"
    }

    async fn run(
        &mut self,
        tx: Sender<AnyUpdate>,
        _rx: Receiver<ConnectorCommand>,
    ) {
        while let Some(event) = self.cursor.next() {
            let update = normalize(event);
            tx.send(update).await.unwrap();
        }
    }
}

