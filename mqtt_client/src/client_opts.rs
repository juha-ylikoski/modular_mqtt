use rust_mqtt_protocol::Qos;

pub struct LastWill {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
    pub qos: Qos,
}

pub enum OnDisconnectBehavior {
    Panic,
}

pub struct ClientOpts {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<LastWill>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub on_disconnect: OnDisconnectBehavior,
}
