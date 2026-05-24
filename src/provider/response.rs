use crate::sink::OutcomeKind;

pub trait ResponseCodec: Send + Sync {
    fn parse(&self, body: &[u8]) -> OutcomeKind;
}

pub struct JsonRpcCodec;

fn json_string_after(body: &[u8], key: &str) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let needle = format!("\"{key}\"");
    let key_pos = text.find(&needle)?;
    let after = &text[key_pos + needle.len()..];
    let colon = after.find(':')?;
    let rest = after[colon + 1..].trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn json_i64_after(body: &[u8], key: &str) -> Option<i64> {
    let text = std::str::from_utf8(body).ok()?;
    let needle = format!("\"{key}\"");
    let key_pos = text.find(&needle)?;
    let after = &text[key_pos + needle.len()..];
    let colon = after.find(':')?;
    let rest = after[colon + 1..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

impl ResponseCodec for JsonRpcCodec {
    fn parse(&self, body: &[u8]) -> OutcomeKind {
        if let Some(sig) = json_string_after(body, "result") {
            return OutcomeKind::Accepted { signature: sig };
        }
        if body.windows(7).any(|w| w == b"\"error\"") {
            let code = json_i64_after(body, "code").unwrap_or(0);
            let message = json_string_after(body, "message").unwrap_or_default();
            return OutcomeKind::Rejected { code, message };
        }
        OutcomeKind::NoResponse
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::OutcomeKind;

    #[test]
    fn parses_jsonrpc_result_signature() {
        let body = br#"{"jsonrpc":"2.0","result":"5abc","id":1}"#;
        match JsonRpcCodec.parse(body) {
            OutcomeKind::Accepted { signature } => assert_eq!(signature, "5abc"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_jsonrpc_error() {
        let body = br#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"blockhash not found"},"id":1}"#;
        match JsonRpcCodec.parse(body) {
            OutcomeKind::Rejected { code, message } => {
                assert_eq!(code, -32002);
                assert_eq!(message, "blockhash not found");
            }
            other => panic!("{other:?}"),
        }
    }
}
