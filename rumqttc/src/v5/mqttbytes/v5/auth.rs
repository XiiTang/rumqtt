use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    len_len, property, read_mqtt_bytes, read_mqtt_string, read_u8, write_remaining_length, Error,
    FixedHeader, PropertyType,
};

/// Auth packet reason code
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReasonCode {
    Success,
    Continue,
    ReAuthenticate,
}

impl AuthReasonCode {
    fn read(bytes: &mut Bytes) -> Result<Self, Error> {
        let reason_code = read_u8(bytes)?;
        let code = match reason_code {
            0x00 => AuthReasonCode::Success,
            0x18 => AuthReasonCode::Continue,
            0x19 => AuthReasonCode::ReAuthenticate,
            _ => return Err(Error::MalformedPacket),
        };

        Ok(code)
    }

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let reason_code = match self {
            AuthReasonCode::Success => 0x00,
            AuthReasonCode::Continue => 0x18,
            AuthReasonCode::ReAuthenticate => 0x19,
        };

        buffer.put_u8(reason_code);

        Ok(())
    }
}

/// Used to perform extended authentication exchange
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub code: AuthReasonCode,
    pub properties: Option<AuthProperties>,
}

impl Auth {
    fn len(&self) -> usize {
        if self.code == AuthReasonCode::Success && self.properties.is_none() {
            return 0;
        }
        let properties_len = self.properties.as_ref().map_or(0, AuthProperties::len);
        1 + len_len(properties_len) + properties_len
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
        Self::read_traced(fixed_header, bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        fixed_header: FixedHeader,
        mut bytes: Bytes,
        order: &mut Vec<u8>,
    ) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        if bytes.is_empty() {
            return Ok(Auth {
                code: AuthReasonCode::Success,
                properties: None,
            });
        }
        let code = AuthReasonCode::read(&mut bytes)?;
        let properties = if bytes.is_empty() {
            None
        } else {
            AuthProperties::read_traced(&mut bytes, order)?
        };
        if !bytes.is_empty() {
            return Err(Error::MalformedPacket);
        }
        let auth = Auth { code, properties };

        Ok(auth)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        self.write_ordered(buffer, None)
    }
    pub(super) fn write_ordered(
        &self,
        buffer: &mut BytesMut,
        order: Option<&[u8]>,
    ) -> Result<usize, Error> {
        buffer.put_u8(0xF0);

        let len = self.len();
        let count = write_remaining_length(buffer, len)?;

        if len == 0 {
            return Ok(1 + count);
        }
        self.code.write(buffer)?;
        if let Some(p) = &self.properties {
            p.write_ordered(buffer, order)?;
        } else {
            write_remaining_length(buffer, 0)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuthProperties {
    pub method: Option<String>,
    pub data: Option<Bytes>,
    pub reason: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl AuthProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if let Some(method) = &self.method {
            let m_len = method.len();
            len += 1 + 2 + m_len;
        }

        if let Some(data) = &self.data {
            let d_len = data.len();
            len += 1 + 2 + d_len;
        }

        if let Some(reason) = &self.reason {
            let r_len = reason.len();
            len += 1 + 2 + r_len;
        }

        for (key, value) in self.user_properties.iter() {
            let p_len = key.len() + value.len();
            len += 1 + 4 + p_len;
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<AuthProperties>, Error> {
        Self::read_traced(bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        bytes: &mut Bytes,
        order: &mut Vec<u8>,
    ) -> Result<Option<AuthProperties>, Error> {
        let mut section = super::read_properties_section(bytes)?;
        let bytes = &mut section;
        if bytes.is_empty() {
            return Ok(None);
        }

        let mut props = AuthProperties::default();

        let mut seen = 0u64;
        let mut count = 0usize;
        while bytes.has_remaining() {
            let prop = read_u8(bytes)?;
            super::validate_property_occurrence(prop, prop == 38, &mut seen, &mut count)?;
            order.push(prop);

            match property(prop)? {
                PropertyType::AuthenticationMethod => {
                    let method = read_mqtt_string(bytes)?;

                    props.method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = read_mqtt_bytes(bytes)?;

                    props.data = Some(data);
                }
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;

                    props.reason = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;

                    props.user_properties.push((key, value));
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(props))
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        self.write_ordered(buffer, None)
    }
    pub(super) fn write_ordered(
        &self,
        buffer: &mut BytesMut,
        order: Option<&[u8]>,
    ) -> Result<(), Error> {
        use super::ordered::{write_properties, Property};
        let count = usize::from(self.method.is_some())
            + usize::from(self.data.is_some())
            + usize::from(self.reason.is_some())
            + self.user_properties.len();
        if count > 1024 {
            return Err(Error::MalformedPacket);
        }
        let mut values = Vec::with_capacity(count);
        if let Some(value) = &self.method {
            values.push((21, Property::String(value)));
        }
        if let Some(value) = &self.data {
            values.push((22, Property::Binary(value)));
        }
        if let Some(value) = &self.reason {
            values.push((31, Property::String(value)));
        }
        for value in &self.user_properties {
            values.push((38, Property::Pair(&value.0, &value.1)));
        }
        write_properties(buffer, values, order)
    }
}
