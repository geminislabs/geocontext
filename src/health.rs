use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub fn spawn_health_server(bind_addr: String) {
    tokio::spawn(async move {
        if let Err(err) = run_health_server(bind_addr.clone()).await {
            error!(error = ?err, bind_addr = %bind_addr, "Health server stopped with error");
        }
    });
}

async fn run_health_server(bind_addr: String) -> Result<()> {
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("Failed to bind health server on {}", bind_addr))?;

    info!(bind_addr = %bind_addr, "Health server listening");

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .context("Health server accept failed")?;
        tokio::spawn(async move {
            if let Err(err) = handle_connection(socket).await {
                error!(error = ?err, "Health request handling failed");
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream) -> Result<()> {
    let mut buf = [0_u8; 1024];
    let n = socket
        .read(&mut buf)
        .await
        .context("Failed to read health request")?;

    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let is_health = request.starts_with("GET /health ") || request.starts_with("GET /ready ");

    let (status, body) = if is_health {
        ("200 OK", r#"{"status":"ok"}"#)
    } else {
        ("404 Not Found", r#"{"status":"not_found"}"#)
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    socket
        .write_all(response.as_bytes())
        .await
        .context("Failed to write health response")?;

    socket
        .flush()
        .await
        .context("Failed to flush health response")?;

    Ok(())
}
