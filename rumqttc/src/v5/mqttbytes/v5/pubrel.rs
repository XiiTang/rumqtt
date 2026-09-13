use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in PubRel
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubRelReason {
    Success,
    PacketIdentifierNotFound,
}

/// QoS2 Publish release, in response to PUBREC packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRel {
    pub pkid: u16,
    pub reason: PubRelReason,
    pub properties: Option<PubRelProperties>,
}

impl PubRel {
    pub fn new(pkid: u16, properties: Option<PubRelProperties>) -> Self {
        Self {
            pkid,
            reason: PubRelReason::Success,
            properties,
        }
    }

    pub fn size(&self) -> usize {
        // If there are no properties during success, sending reason code is optional
        if self.reason == PubRelReason::Success && self.properties.is_none() {
            return 4;
        }

        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    fn len(&self) -> usize {
        let mut len = 2 + 1; // pkid + reason

        // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
        // and there are no Properties. In this case the PUBREL has a Remaining Length of 2.
        // <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144>
        if self.reason == PubRelReason::Success && self.properties.is_none() {
            return 2;
        }

        if let Some(p) = &self.properties {
            let properties_len = p.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            len += 1;
        }

        len
    }

    pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<PubRel, Error> {
        Self::read_traced(fixed_header, bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        fixed_header: FixedHeader,
        mut bytes: Bytes,
        order: &mut Vec<u8>,
    ) -> Result<PubRel, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let pkid = read_u16(&mut bytes)?;
        if fixed_header.remaining_len == 2 {
            return Ok(PubRel {
                pkid,
                reason: PubRelReason::Success,
                properties: None,
            });
        }

        let ack_reason = read_u8(&mut bytes)?;
        if fixed_header.remaining_len < 4 {
            return Ok(PubRel {
                pkid,
                reason: reason(ack_reason)?,
                properties: None,
            });
        }

        let properties = PubRelProperties::read_traced(&mut bytes, order)?;
        let puback = PubRel {
            pkid,
            reason: reason(ack_reason)?,
            properties,
        };

        if !bytes.is_empty() {
            return Err(Error::MalformedPacket);
        }
        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x62);
        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);

        // If there are no properties during success, sending reason code is optional
        if self.reason == PubRelReason::Success && self.properties.is_none() {
            return Ok(4);
        }

        buffer.put_u8(code(self.reason));

        if let Some(p) = &self.properties {
            p.write(buffer)?;
        } else {
            write_remaining_length(buffer, 0)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRelProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl PubRelProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if let Some(reason) = &self.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<PubRelProperties>, Error> {
        Self::read_traced(bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        bytes: &mut Bytes,
        order: &mut Vec<u8>,
    ) -> Result<Option<PubRelProperties>, Error> {
        let mut reason_string = None;
        let mut user_properties = Vec::new();

        let mut section = super::read_properties_section(bytes)?;
        let bytes = &mut section;
        if bytes.is_empty() {
            return Ok(None);
        }

        let mut seen = 0u64;
        let mut count = 0usize;
        while bytes.has_remaining() {
            let prop = read_u8(bytes)?;
            super::validate_property_occurrence(prop, prop == 38, &mut seen, &mut count)?;
            order.push(prop);

            match property(prop)? {
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;

                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;

                    user_properties.push((key, value));
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(PubRelProperties {
            reason_string,
            user_properties,
        }))
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(reason) = &self.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in self.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        Ok(())
    }
}

/// Connection return code type
fn reason(num: u8) -> Result<PubRelReason, Error> {
    let code = match num {
        0 => PubRelReason::Success,
        146 => PubRelReason::PacketIdentifierNotFound,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

fn code(reason: PubRelReason) -> u8 {
    match reason {
        PubRelReason::Success => 0,
        PubRelReason::PacketIdentifierNotFound => 146,
    }
}

#[cfg(test)]
mod test {
    use super::super::test::{USER_PROP_KEY, USER_PROP_VAL};
    use super::*;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn length_calculation() {
        let mut dummy_bytes = BytesMut::new();
        // Use user_properties to pad the size to exceed ~128 bytes to make the
        // remaining_length field in the packet be 2 bytes long.
        let pubrel_props = PubRelProperties {
            reason_string: None,
            user_properties: vec![(USER_PROP_KEY.into(), USER_PROP_VAL.into())],
        };

        let pubrel_pkt = PubRel::new(1, Some(pubrel_props));

        let size_from_size = pubrel_pkt.size();
        let size_from_write = pubrel_pkt.write(&mut dummy_bytes).unwrap();
        let size_from_bytes = dummy_bytes.len();

        assert_eq!(size_from_write, size_from_bytes);
        assert_eq!(size_from_size, size_from_bytes);
    }
}

impl From<PubRelReason> for u8 {
    fn from(value: PubRelReason) -> Self {
        code(value)
    }
}
impl TryFrom<u8> for PubRelReason {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        reason(value)
    }
}
