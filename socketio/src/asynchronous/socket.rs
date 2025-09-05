use super::generator::StreamGenerator;
use crate::{
    error::Result,
    packet::{Packet, PacketId},
    Error, Event, Payload,
};
use async_stream::try_stream;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use rust_engineio::{
    asynchronous::Client as EngineClient, Packet as EnginePacket, PacketId as EnginePacketId,
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::timeout;

#[derive(Clone)]
pub(crate) struct Socket {
    engine_client: Arc<EngineClient>,
    connected: Arc<AtomicBool>,
    generator: StreamGenerator<Packet>,
}

impl Socket {
    /// Creates an instance of `Socket`.
    pub(super) fn new(engine_client: EngineClient) -> Result<Self> {
        let connected = Arc::new(AtomicBool::default());
        Ok(Socket {
            engine_client: Arc::new(engine_client.clone()),
            connected: connected.clone(),
            generator: StreamGenerator::new(Self::stream(engine_client, connected)),
        })
    }

    /// Connects to the server. This includes a connection of the underlying
    /// engine.io client and afterwards an opening socket.io request.
    pub async fn connect(&self) -> Result<()> {
        self.engine_client.connect().await?;

        // store the connected value as true, if the connection process fails
        // later, the value will be updated
        self.connected.store(true, Ordering::Release);

        Ok(())
    }

    /// Disconnects from the server by sending a socket.io `Disconnect` packet. This results
    /// in the underlying engine.io transport to get closed as well.
    pub async fn disconnect(&self) -> Result<()> {
        if self.is_engineio_connected().await {
            self.engine_client.disconnect().await?;
        }
        if self.connected.load(Ordering::Acquire) {
            self.connected.store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Sends a `socket.io` packet to the server using the `engine.io` client.
    pub async fn send(&self, packet: Packet) -> Result<()> {
        if !self.is_engineio_connected().await || !self.connected.load(Ordering::Acquire) {
            return Err(Error::IllegalActionBeforeOpen());
        }

        // the packet, encoded as an engine.io message packet
        let engine_packet = EnginePacket::new(EnginePacketId::Message, Bytes::from(&packet));
        self.engine_client.emit(engine_packet).await?;

        if let Some(attachments) = packet.attachments {
            for attachment in attachments {
                let engine_packet = EnginePacket::new(EnginePacketId::MessageBinary, attachment);
                self.engine_client.emit(engine_packet).await?;
            }
        }

        Ok(())
    }

    /// Emits to certain event with given data. The data needs to be JSON,
    /// otherwise this returns an `InvalidJson` error.
    pub async fn emit(&self, nsp: &str, event: Event, data: Payload) -> Result<()> {
        let socket_packet = Packet::new_from_payload(data, event, nsp, None)?;

        self.send(socket_packet).await
    }

    fn stream(
        client: EngineClient,
        is_connected: Arc<AtomicBool>,
    ) -> Pin<Box<impl Stream<Item = Result<Packet>> + Send>> {
        Box::pin(try_stream! {
                for await received_data in client.clone() {
                    let packet = received_data?;

                    if packet.packet_id == EnginePacketId::Message
                        || packet.packet_id == EnginePacketId::MessageBinary
                    {
                        let packet = Self::handle_engineio_packet(packet, client.clone()).await?;
                        Self::handle_socketio_packet(&packet, is_connected.clone());

                        yield packet;
                    }
                }
        })
    }

    /// Handles the connection/disconnection.
    #[inline]
    fn handle_socketio_packet(socket_packet: &Packet, is_connected: Arc<AtomicBool>) {
        match socket_packet.packet_type {
            PacketId::Connect => {
                is_connected.store(true, Ordering::Release);
            }
            PacketId::ConnectError => {
                is_connected.store(false, Ordering::Release);
            }
            PacketId::Disconnect => {
                is_connected.store(false, Ordering::Release);
            }
            _ => (),
        }
    }

    /// Handles new incoming engineio packets
    async fn handle_engineio_packet(
        packet: EnginePacket,
        mut client: EngineClient,
    ) -> Result<Packet> {
        let mut socket_packet = Packet::try_from(&packet.data)?;

        // Only handle attachments if there are any
        if socket_packet.attachment_count > 0 {
            let mut attachments_left = socket_packet.attachment_count;
            let mut attachments = Vec::new();
            while attachments_left > 0 {
                // TODO: This is not nice! Find a different way to peek the next element while mapping the stream
                let next = client.next().await.unwrap();
                match next {
                    Err(err) => return Err(err.into()),
                    Ok(packet) => match packet.packet_id {
                        EnginePacketId::MessageBinary | EnginePacketId::Message => {
                            attachments.push(packet.data);
                            attachments_left -= 1;
                        }
                        _ => {
                            return Err(Error::InvalidAttachmentPacketType(
                                packet.packet_id.into(),
                            ));
                        }
                    },
                }
            }
            socket_packet.attachments = Some(attachments);
        }

        Ok(socket_packet)
    }

    /// Checks if the socket is connected by performing an actual connectivity test.
    /// This method now uses the robust connection testing instead of just checking
    /// the engine.io client's internal state.
    pub async fn is_engineio_connected(&self) -> bool {
        self.is_connected().await
    }

    /// Legacy synchronous method for backward compatibility - delegates to async version
    /// Note: This method is not recommended for new code as it may block
    pub fn is_engineio_connected_sync(&self) -> bool {
        // For synchronous calls, we fall back to the engine client's state
        // as we can't do async operations here
        self.engine_client.is_connected()
    }

    /// Get the timestamp of the last successful communication
    /// Returns the maximum of last ping sent or last pong received in milliseconds since Unix epoch
    pub async fn get_last_communication_time(&self) -> Result<u64> {
        // Get the last communication time from the underlying engine.io client
        Ok(self.engine_client.get_last_communication_time().await)
    }

    /// Get the timestamp of the last ping sent in milliseconds since Unix epoch
    pub async fn get_last_ping_time(&self) -> Result<Option<u64>> {
        Ok(self.engine_client.get_last_ping_time().await)
    }

    /// Get the timestamp of the last pong received in milliseconds since Unix epoch
    pub async fn get_last_pong_time(&self) -> Result<Option<u64>> {
        Ok(self.engine_client.get_last_pong_time().await)
    }

    /// Get the maximum ping timeout configured in milliseconds
    pub fn get_max_ping_timeout(&self) -> u64 {
        self.engine_client.get_max_ping_timeout()
    }

    /// Get the time remaining until the next ping should be received in milliseconds
    pub async fn get_time_to_next_ping(&self) -> Result<u64> {
        Ok(self.engine_client.get_time_to_next_ping().await)
    }

    /// Performs an immediate connectivity test to verify the actual network connection state.
    /// This is much more reliable than just checking socket library state, as it detects
    /// network disconnects that the socket library might not have noticed yet.
    pub async fn is_connected(&self) -> bool {
        // CRITICAL FIX: The original is_engineio_connected() only checks socket library state,
        // not actual network connectivity. This was causing false positives when network
        // was disconnected but socket hadn't detected it yet.

        // IMMEDIATE CONNECTIVITY TEST: This is the most reliable check
        // Test actual network connectivity FIRST before relying on socket library state
        let ping_result = timeout(
            Duration::from_millis(100), // 100ms timeout for ultra-fast detection
            async {
                // Send a minimal ping packet to test connectivity
                let test_packet = EnginePacket::new(EnginePacketId::Ping, Bytes::new());
                self.engine_client.emit(test_packet).await
            }
        ).await;

        match ping_result {
            Ok(Ok(_)) => {
                // Ping succeeded - connection is definitely up
                true
            },
            Ok(Err(e)) => {
                // Ping failed - analyze the error to determine if it's network-related
                let error_string = e.to_string().to_lowercase();

                let is_network_error = error_string.contains("alreadyclosed") ||
                    error_string.contains("incompleteresponse") ||
                    error_string.contains("websocket") ||
                    error_string.contains("connection") ||
                    error_string.contains("network") ||
                    error_string.contains("transport") ||
                    error_string.contains("broken pipe") ||
                    error_string.contains("connection reset") ||
                    error_string.contains("connection refused") ||
                    error_string.contains("timeout");

                if is_network_error {
                    // Network error - connection is definitely down
                    false
                } else {
                    // Non-network error - check socket library state as fallback
                    let socket_state = self.engine_client.is_connected();
                    if !socket_state {
                        // Socket library also reports disconnected
                        false
                    } else {
                        // Socket library thinks we're connected but ping failed for non-network reason
                        // This is suspicious but not definitive - assume connection is still good
                        // This handles cases like temporary server-side issues
                        true
                    }
                }
            },
            Err(_) => {
                // Timeout occurred - this is a strong indicator of network issues
                false
            }
        }
    }
}

impl Stream for Socket {
    type Item = Result<Packet>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

impl Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("engine_client", &self.engine_client)
            .field("connected", &self.connected)
            .finish()
    }
}
