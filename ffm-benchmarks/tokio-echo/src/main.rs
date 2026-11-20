// Reference tokio echo server (multi-threaded runtime, one task per connection, 16 KiB buffer).
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::args().nth(1).map(|p| p.parse().unwrap()).unwrap_or(9000);
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    println!("tokio echo port={port}");
    loop {
        let (mut sock, _) = listener.accept().await?;
        sock.set_nodelay(true)?;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16384];
            loop {
                let n = match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                if sock.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        });
    }
}
