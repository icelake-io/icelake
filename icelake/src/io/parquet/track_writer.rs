use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
};

use opendal::Writer;
use tokio::io::AsyncWrite;

/// `TrackWriter` is used to track the written size.
pub struct TrackWriter {
    writer: Writer,
    written_size: Arc<AtomicU64>,
}

impl TrackWriter {
    pub fn new(writer: Writer) -> Self {
        Self {
            writer,
            written_size: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return a reference to the written size, it can be used to track the written size.
    pub fn get_wrriten_size(&self) -> Arc<AtomicU64> {
        self.written_size.clone()
    }
}

impl AsyncWrite for TrackWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match Pin::new(&mut self.writer).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => {
                self.written_size
                    .fetch_add(buf.len() as u64, std::sync::atomic::Ordering::SeqCst);
                std::task::Poll::Ready(Ok(n))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}
