use crate::protocol::http::envelope::{BodyPlaceholder, ContentLengthSpec, EnvelopeSpec};

pub enum AuthPlacement {
    None,
    Header { name: String, value: String },
    UrlParam { key: &'static str, value: String },
    UrlPath { token: String },
}

pub struct EnvelopeSpecBuilder {
    http_method: &'static str,
    base_path: String,
    host: String,
    extra_headers: Vec<(String, String)>,
    body_tpl: String,
    max_body: usize,
    auth: AuthPlacement,
}

const DEFAULT_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true,"maxRetries":0}]}"#;

impl EnvelopeSpecBuilder {
    #[must_use]
    pub fn new(method: &'static str, path: &str, host: &str, max_body: usize) -> Self {
        Self {
            http_method: method,
            base_path: path.to_string(),
            host: host.to_string(),
            extra_headers: Vec::new(),
            body_tpl: DEFAULT_BODY.to_string(),
            max_body,
            auth: AuthPlacement::None,
        }
    }

    #[must_use]
    pub fn send_transaction(path: &str, host: &str, max_body: usize) -> Self {
        Self::new("POST", path, host, max_body)
    }

    #[must_use]
    pub fn auth_placement(mut self, a: AuthPlacement) -> Self {
        self.auth = a;
        self
    }

    #[must_use]
    pub fn body_template(mut self, tpl: &str) -> Self {
        self.body_tpl = tpl.to_string();
        self
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.extra_headers.push((name.to_string(), value.to_string()));
        self
    }

    #[must_use]
    pub fn build(self) -> EnvelopeSpec {
        use std::fmt::Write;
        let path = match &self.auth {
            AuthPlacement::UrlParam { key, value } => {
                format!("{}?{}={}", self.base_path, key, value)
            }
            AuthPlacement::UrlPath { token } => {
                format!("{}/{}", self.base_path.trim_end_matches('/'), token)
            }
            _ => self.base_path.clone(),
        };
        let mut wire = String::new();
        let _ = write!(wire, "{} {} HTTP/1.1\r\n", self.http_method, path);
        let _ = write!(wire, "Host: {}\r\n", self.host);
        wire.push_str("Content-Type: application/json\r\n");
        if let AuthPlacement::Header { name, value } = &self.auth {
            let _ = write!(wire, "{name}: {value}\r\n");
        }
        for (k, v) in &self.extra_headers {
            let _ = write!(wire, "{k}: {v}\r\n");
        }
        wire.push_str("Content-Length: <<CL>>      \r\n");
        wire.push_str("\r\n");
        wire.push_str(&self.body_tpl);

        EnvelopeSpec {
            bytes: wire.into_bytes(),
            body: BodyPlaceholder {
                sentinel: b"<<BODY>>".to_vec(),
                max_len: self.max_body,
            },
            content_length: Some(ContentLengthSpec {
                sentinel: b"<<CL>>      ".to_vec(),
                width: 12,
            }),
        }
    }
}
