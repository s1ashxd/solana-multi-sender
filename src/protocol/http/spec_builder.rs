use crate::protocol::http::envelope::{
    BodyPlaceholder, ContentLengthSpec, EnvelopeSpec,
};

pub struct EnvelopeSpecBuilder {
    path: String,
    host: String,
    headers: Vec<(String, String)>,
    body_template: String,
    max_body: usize,
}

impl EnvelopeSpecBuilder {
    #[must_use]
    pub fn send_transaction(path: &str, host: &str, max_body: usize) -> Self {
        Self {
            path: path.to_string(),
            host: host.to_string(),
            headers: Vec::new(),
            body_template: r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true,"maxRetries":0}]}"#.to_string(),
            max_body,
        }
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    #[must_use]
    pub fn build(self) -> EnvelopeSpec {
        use std::fmt::Write;
        let mut wire = String::new();
        let _ = write!(wire, "POST {} HTTP/1.1\r\n", self.path);
        let _ = write!(wire, "Host: {}\r\n", self.host);
        let _ = write!(wire, "Content-Type: application/json\r\n");
        for (k, v) in &self.headers {
            let _ = write!(wire, "{k}: {v}\r\n");
        }
        wire.push_str("Content-Length: <<CL>>      \r\n");
        wire.push_str("\r\n");
        wire.push_str(&self.body_template);

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
