use crate::error::EnvelopeError;

#[derive(Debug, Clone)]
pub struct BodyPlaceholder {
    pub(crate) sentinel: Vec<u8>,
    pub(crate) max_len: usize,
}

#[derive(Debug, Clone)]
pub struct ContentLengthSpec {
    pub(crate) sentinel: Vec<u8>,
    pub(crate) width: u8,
}

#[derive(Debug, Clone)]
pub struct EnvelopeSpec {
    pub(crate) bytes: Vec<u8>,
    pub(crate) body: BodyPlaceholder,
    pub(crate) content_length: Option<ContentLengthSpec>,
}

fn find_unique(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let mut found = None;
    let mut i = 0;
    while i + needle.len() <= haystack.len() {
        if &haystack[i..i + needle.len()] == needle {
            if found.is_some() {
                return None;
            }
            found = Some(i);
        }
        i += 1;
    }
    found
}

pub struct EnvelopeTemplate {
    buf: Vec<u8>,
    out: Vec<u8>,
    body_off: u32,
    body_max: u32,
    cl_off: Option<u32>,
    cl_width: u8,
    http_body_start: u32,
}

impl EnvelopeSpec {
    pub fn compile(self) -> Result<EnvelopeTemplate, EnvelopeError> {
        EnvelopeTemplate::compile(self)
    }
}

impl EnvelopeTemplate {
    pub fn compile(mut spec: EnvelopeSpec) -> Result<Self, EnvelopeError> {
        let sentinel_len = spec.body.sentinel.len();
        if spec.body.max_len < sentinel_len {
            return Err(EnvelopeError::BodyMaxTooSmall {
                max_len: spec.body.max_len,
                sentinel: sentinel_len,
            });
        }
        let body_off =
            find_unique(&spec.bytes, &spec.body.sentinel).ok_or(EnvelopeError::BodySentinelMissing)?;

        let (mut cl_off, cl_width) = if let Some(ref cl) = spec.content_length {
            let off = find_unique(&spec.bytes, &cl.sentinel).ok_or(EnvelopeError::BodySentinelMissing)?;
            (Some(u32::try_from(off).expect("cl off fits")), cl.width)
        } else {
            (None, 0)
        };

        let max_len = spec.body.max_len;
        if max_len > sentinel_len {
            let mut padded = Vec::with_capacity(spec.bytes.len() + (max_len - sentinel_len));
            padded.extend_from_slice(&spec.bytes[..body_off]);
            padded.extend(std::iter::repeat_n(b' ', max_len));
            padded.extend_from_slice(&spec.bytes[body_off + sentinel_len..]);
            spec.bytes = padded;
            let delta = u32::try_from(max_len - sentinel_len).expect("delta fits");
            let shift_threshold = u32::try_from(body_off + sentinel_len).expect("threshold fits");
            if let Some(ref mut off) = cl_off {
                if *off >= shift_threshold {
                    *off += delta;
                }
            }
        }

        let body_max = u32::try_from(max_len).expect("body_max fits");
        let body_off_u32 = u32::try_from(body_off).expect("body off fits");

        let header_sep = spec.bytes
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or(EnvelopeError::BodySentinelMissing)?;
        let http_body_start = u32::try_from(header_sep + 4).expect("http body start fits");

        let cap = spec.bytes.len();
        Ok(Self {
            buf: spec.bytes,
            out: Vec::with_capacity(cap),
            body_off: body_off_u32,
            body_max,
            cl_off,
            cl_width,
            http_body_start,
        })
    }

    pub fn splice(&mut self, tx: &[u8]) -> Result<&[u8], EnvelopeError> {
        let body_off = self.body_off as usize;
        let body_max = self.body_max as usize;

        let needed = tx.len().div_ceil(3) * 4;
        if needed > body_max {
            return Err(EnvelopeError::BodyTooLarge { encoded: needed, body_max });
        }

        self.out.clear();
        self.out.extend_from_slice(&self.buf[..body_off]);

        let body_start = self.out.len();
        self.out.resize(body_start + needed, 0);
        let encoded_len = base64_simd::STANDARD
            .encode(tx, base64_simd::Out::from_slice(&mut self.out[body_start..]))
            .len();
        self.out.truncate(body_start + encoded_len);

        self.out.extend_from_slice(&self.buf[body_off + body_max..]);
        let real_len = self.out.len();

        if let Some(cl_off) = self.cl_off {
            let cl_off_usize = cl_off as usize;
            let cl_width = self.cl_width as usize;
            let http_body_len = real_len - self.http_body_start as usize;

            let mut scratch = [0u8; 20];
            let mut n = http_body_len;
            let mut pos = scratch.len();
            loop {
                pos -= 1;
                scratch[pos] = b'0' + (n % 10) as u8;
                n /= 10;
                if n == 0 {
                    break;
                }
            }
            let digits = &scratch[pos..];

            if digits.len() > cl_width {
                return Err(EnvelopeError::ContentLengthOverflow {
                    value: http_body_len,
                    width: self.cl_width,
                });
            }
            self.out[cl_off_usize..cl_off_usize + digits.len()].copy_from_slice(digits);
            for b in &mut self.out[cl_off_usize + digits.len()..cl_off_usize + cl_width] {
                *b = b' ';
            }
        }

        Ok(&self.out[..real_len])
    }
}
