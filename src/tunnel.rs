pub struct TunnelServer {
    channel_handle: muzanci_transport::channel::ChannelHandle,
}

impl TunnelServer {
    pub fn new(channel_handle: muzanci_transport::channel::ChannelHandle) -> Self {
        Self { channel_handle }
    }

    pub async fn run(&mut self) {
        loop {
            match self.channel_handle.recv().await {
                Some(message) => {
                    println!("Tunnel server received message: {:?}", message);
                    self.handle_message(message).await;
                }
                None => {
                    eprintln!("Tunnel server channel closed");
                    break;
                }
            }
        }
    }

    pub async fn handle_message(&mut self, message: muzanci_transport::channel::Message) {
        println!("Tunnel server handling message: {:?}", message);
        unimplemented!("Tunnel server message handling not implemented yet");
    }
}
