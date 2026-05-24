pub mod response;

use std::sync::Arc;

use crate::provider::response::ResponseCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Http,
}

#[derive(Clone, Debug)]
pub enum HttpAuth {
    None,
    Header { name: String, value: String },
}

#[derive(Clone, Debug)]
pub struct HttpEndpoint {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub server_name: String,
}

#[derive(Clone)]
pub struct ProviderConfig {
    pub protocol: Protocol,
    pub endpoint: HttpEndpoint,
    pub auth: HttpAuth,
    pub max_body: usize,
    pub codec: Arc<dyn ResponseCodec>,
}
