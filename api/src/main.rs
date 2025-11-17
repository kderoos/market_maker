use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use common::{TradeUpdate,BookUpdate, ConnectorCommand, ExchangeConnectorCommand};
use engine::Engine;
use futures::{SinkExt, StreamExt};
use serde::{Serialize,Deserialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;
// use tracing_subscriber;



#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::init();
    let engine = Arc::new(Engine::init());
    
    // Broadcast channel for outgoing updates
    // let (tx, _rx) = broadcast::channel::<AnyWsUpdate>(100);
    let tx = engine.tx_ws.clone();
    // Clone for the HTTP route
    // let tx_clone = tx.clone();

    // Spawn a dummy task that simulates book updates
    // tokio::spawn(simulate_book_updates(tx));

    // WebSocket route
    let app = Router::new().route("/ws", get(move |ws| ws_handler(ws, engine.clone())));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🚀 WebSocket server running on ws://{addr}/ws");

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, engine: Arc<Engine>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, engine.clone()))
}

async fn handle_socket(mut socket: WebSocket, engine: Arc<Engine>) {
    println!("🟢 New WebSocket client connected");

    // Subscibe to broadcast channel for this client
    let mut rx = engine.tx_ws.subscribe();

    loop {
        tokio::select!{
            result = rx.recv() => {
                match result {
                    Ok(update) => {
                        let msg = serde_json::to_string(&update).unwrap();
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            println!("Client disconnected");
                            break;
                        }
                    }
                    Err(e) => {
                        println!("Error receiving from channel: {}", e);
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        println!("Client said: {}", text);
                        // Try to parse as ExchangeConnectorCommand
                        match serde_json::from_str::<ExchangeConnectorCommand>(&text) {
                            Ok(cmd) => {
                                println!("Parsed command: {:?}", cmd);
                                //Currently send to all exchanges, improve later
                                println!("Sent to engine: {:?}", cmd.cmd);
                                engine.send_cmd(cmd.cmd);
                            }
                            Err(e) => {
                                println!("Failed to parse command: {}", e);
                                // Send error message back to client
                                let err_msg = format!("Error parsing command: {}", e);
                                socket.send(Message::Text(err_msg.into())).await.unwrap();
                            }
                        }      
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        println!("Error receiving message from client: {}", e);
                        break;
                    }
                    None => {
                        println!("Client disconnected");
                        break;
                    }
                }
            }
        }
    }
    println!("🔴 Client disconnected");
}
    // // Spawn a task to receive updates and foreward them to client
    // let mut send_task = tokio::spawn(async move {
    //     while let Ok(update) = rx.recv().await {
    //         let msg = serde_json::to_string(&update).unwrap();
    //         if socket.send(Message::Text(msg.into())).await.is_err() {
    //             println!("Client disconnected");
    //             break;
    //         }
    //     }
    // });

    // // Optionally handle client messages (like subscriptions)
    // let mut recv_task = tokio::spawn(async move {
    //     while let Some(Ok(msg)) = socket.next().await {
    //         if let Message::Text(text) = msg {
    //             println!("Client said: {}", text);
    //             // Try to parse as ExchangeConnectorCommand
    //             match serde_json::from_str::<ExchangeConnectorCommand>(&text) {
    //                 Ok(cmd) => {
    //                     println!("Parsed command: {:?}", cmd);
    //                     //Currently send to all exchanges, improve later
    //                     engine.send_cmd(cmd.cmd);
    //                 }
    //                 Err(e) => {
    //                     println!("Failed to parse command: {}", e);
    //                     // Send error message back to client
    //                     let err_msg = format!("Error parsing command: {}", e);
    //                     socket.send(Message::Text(err_msg.into())).await.unwrap();
    //                 }
    //             }      
    //         }
    //     }
    // });

    // // Stop both when either ends
    // tokio::select! {
    //     _ = (&mut send_task) => recv_task.abort(),
    //     _ = (&mut recv_task) => send_task.abort(),
    // }



// // Dummy task: sends fake book updates every 2 seconds
// async fn simulate_book_updates(tx: broadcast::Sender<BookDepth>) {
//     let mut bid = 50000.0;
//     let mut ask = 50010.0;
//     loop {
//         let update = BookDepth {
//             best_bid: bid,
//             best_ask: ask,
//             timestamp: chrono::Utc::now().timestamp_micros(),
//         };
//         let _ = tx.send(update);

//         bid += (rand::random::<f64>() - 0.5) * 10.0;
//         ask = bid + 10.0;
//         tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
//     }
// }

