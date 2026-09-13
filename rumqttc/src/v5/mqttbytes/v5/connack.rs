use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
// This contains return codes for both MQTT v311 and v5
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectReturnCode {
    Success,
    RefusedProtocolVersion,
    BadClientId,
    ServiceUnavailable,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUserNamePassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    BadAuthenticationMethod,
    TopicNameInvalid,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QoSNotSupported,
    UseAnotherServer,
    ServerMoved,
    ConnectionRateExceeded,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
    pub properties: Option<ConnAckProperties>,
}

impl ConnAck {
    fn len(&self) -> usize {
        let mut len = 1  // session present
                    + 1; // code

        if let Some(p) = &self.properties {
            let properties_len = p.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            len += 1;
        }

        len
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<ConnAck, Error> {
        Self::read_traced(fixed_header, bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        fixed_header: FixedHeader,
        mut bytes: Bytes,
        order: &mut Vec<u8>,
    ) -> Result<ConnAck, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let flags = read_u8(&mut bytes)?;
        let return_code = read_u8(&mut bytes)?;
        let properties = ConnAckProperties::read_traced(&mut bytes, order)?;

        if flags > 1 || return_code != 0 && flags != 0 {
            return Err(Error::MalformedPacket);
        }
        let session_present = (flags & 0x01) == 1;
        let code = connect_return(return_code)?;
        let connack = ConnAck {
            session_present,
            code,
            properties,
        };

        if !bytes.is_empty() {
            return Err(Error::MalformedPacket);
        }
        Ok(connack)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        self.write_ordered(buffer, None)
    }
    pub(super) fn write_ordered(
        &self,
        buffer: &mut BytesMut,
        order: Option<&[u8]>,
    ) -> Result<usize, Error> {
        let len = Self::len(self);
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(self.session_present as u8);
        buffer.put_u8(connect_code(self.code));

        if let Some(p) = &self.properties {
            p.write_ordered(buffer, order)?;
        } else {
            write_remaining_length(buffer, 0)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifiers_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
}

impl ConnAckProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if self.session_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if self.receive_max.is_some() {
            len += 1 + 2;
        }

        if self.max_qos.is_some() {
            len += 1 + 1;
        }

        if self.retain_available.is_some() {
            len += 1 + 1;
        }

        if self.max_packet_size.is_some() {
            len += 1 + 4;
        }

        if let Some(id) = &self.assigned_client_identifier {
            len += 1 + 2 + id.len();
        }

        if self.topic_alias_max.is_some() {
            len += 1 + 2;
        }

        if let Some(reason) = &self.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        if self.wildcard_subscription_available.is_some() {
            len += 1 + 1;
        }

        if self.subscription_identifiers_available.is_some() {
            len += 1 + 1;
        }

        if self.shared_subscription_available.is_some() {
            len += 1 + 1;
        }

        if self.server_keep_alive.is_some() {
            len += 1 + 2;
        }

        if let Some(info) = &self.response_information {
            len += 1 + 2 + info.len();
        }

        if let Some(reference) = &self.server_reference {
            len += 1 + 2 + reference.len();
        }

        if let Some(authentication_method) = &self.authentication_method {
            len += 1 + 2 + authentication_method.len();
        }

        if let Some(authentication_data) = &self.authentication_data {
            len += 1 + 2 + authentication_data.len();
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<ConnAckProperties>, Error> {
        Self::read_traced(bytes, &mut Vec::new())
    }

    pub(super) fn read_traced(
        bytes: &mut Bytes,
        order: &mut Vec<u8>,
    ) -> Result<Option<ConnAckProperties>, Error> {
        let mut session_expiry_interval = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        let mut max_packet_size = None;
        let mut assigned_client_identifier = None;
        let mut topic_alias_max = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut wildcard_subscription_available = None;
        let mut subscription_identifiers_available = None;
        let mut shared_subscription_available = None;
        let mut server_keep_alive = None;
        let mut response_information = None;
        let mut server_reference = None;
        let mut authentication_method = None;
        let mut authentication_data = None;

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
                PropertyType::SessionExpiryInterval => {
                    session_expiry_interval = Some(read_u32(bytes)?);
                }
                PropertyType::ReceiveMaximum => {
                    receive_max = Some(read_u16(bytes)?);
                }
                PropertyType::MaximumQos => {
                    max_qos = Some(read_u8(bytes)?);
                }
                PropertyType::RetainAvailable => {
                    retain_available = Some(read_u8(bytes)?);
                }
                PropertyType::AssignedClientIdentifier => {
                    let id = read_mqtt_string(bytes)?;

                    assigned_client_identifier = Some(id);
                }
                PropertyType::MaximumPacketSize => {
                    max_packet_size = Some(read_u32(bytes)?);
                }
                PropertyType::TopicAliasMaximum => {
                    topic_alias_max = Some(read_u16(bytes)?);
                }
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;

                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;

                    user_properties.push((key, value));
                }
                PropertyType::WildcardSubscriptionAvailable => {
                    wildcard_subscription_available = Some(read_u8(bytes)?);
                }
                PropertyType::SubscriptionIdentifierAvailable => {
                    subscription_identifiers_available = Some(read_u8(bytes)?);
                }
                PropertyType::SharedSubscriptionAvailable => {
                    shared_subscription_available = Some(read_u8(bytes)?);
                }
                PropertyType::ServerKeepAlive => {
                    server_keep_alive = Some(read_u16(bytes)?);
                }
                PropertyType::ResponseInformation => {
                    let info = read_mqtt_string(bytes)?;

                    response_information = Some(info);
                }
                PropertyType::ServerReference => {
                    let reference = read_mqtt_string(bytes)?;

                    server_reference = Some(reference);
                }
                PropertyType::AuthenticationMethod => {
                    let method = read_mqtt_string(bytes)?;

                    authentication_method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = read_mqtt_bytes(bytes)?;

                    authentication_data = Some(data);
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(ConnAckProperties {
            session_expiry_interval,
            receive_max,
            max_qos,
            retain_available,
            max_packet_size,
            assigned_client_identifier,
            topic_alias_max,
            reason_string,
            user_properties,
            wildcard_subscription_available,
            subscription_identifiers_available,
            shared_subscription_available,
            server_keep_alive,
            response_information,
            server_reference,
            authentication_method,
            authentication_data,
        }))
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
        let count = usize::from(self.session_expiry_interval.is_some())
            + usize::from(self.receive_max.is_some())
            + usize::from(self.max_qos.is_some())
            + usize::from(self.retain_available.is_some())
            + usize::from(self.max_packet_size.is_some())
            + usize::from(self.assigned_client_identifier.is_some())
            + usize::from(self.topic_alias_max.is_some())
            + usize::from(self.reason_string.is_some())
            + self.user_properties.len()
            + usize::from(self.wildcard_subscription_available.is_some())
            + usize::from(self.subscription_identifiers_available.is_some())
            + usize::from(self.shared_subscription_available.is_some())
            + usize::from(self.server_keep_alive.is_some())
            + usize::from(self.response_information.is_some())
            + usize::from(self.server_reference.is_some())
            + usize::from(self.authentication_method.is_some())
            + usize::from(self.authentication_data.is_some());
        if count > 1024 {
            return Err(Error::MalformedPacket);
        }
        let mut values = Vec::with_capacity(count);
        if let Some(value) = &self.session_expiry_interval {
            values.push((17, Property::Integer(*value)));
        }
        if let Some(value) = &self.receive_max {
            values.push((33, Property::Short(*value)));
        }
        if let Some(value) = &self.max_qos {
            values.push((36, Property::Byte(*value)));
        }
        if let Some(value) = &self.retain_available {
            values.push((37, Property::Byte(*value)));
        }
        if let Some(value) = &self.max_packet_size {
            values.push((39, Property::Integer(*value)));
        }
        if let Some(value) = &self.assigned_client_identifier {
            values.push((18, Property::String(value)));
        }
        if let Some(value) = &self.topic_alias_max {
            values.push((34, Property::Short(*value)));
        }
        if let Some(value) = &self.reason_string {
            values.push((31, Property::String(value)));
        }
        for value in &self.user_properties {
            values.push((38, Property::Pair(&value.0, &value.1)));
        }
        if let Some(value) = &self.wildcard_subscription_available {
            values.push((40, Property::Byte(*value)));
        }
        if let Some(value) = &self.subscription_identifiers_available {
            values.push((41, Property::Byte(*value)));
        }
        if let Some(value) = &self.shared_subscription_available {
            values.push((42, Property::Byte(*value)));
        }
        if let Some(value) = &self.server_keep_alive {
            values.push((19, Property::Short(*value)));
        }
        if let Some(value) = &self.response_information {
            values.push((26, Property::String(value)));
        }
        if let Some(value) = &self.server_reference {
            values.push((28, Property::String(value)));
        }
        if let Some(value) = &self.authentication_method {
            values.push((21, Property::String(value)));
        }
        if let Some(value) = &self.authentication_data {
            values.push((22, Property::Binary(value)));
        }
        write_properties(buffer, values, order)
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    let code = match num {
        0 => ConnectReturnCode::Success,
        128 => ConnectReturnCode::UnspecifiedError,
        129 => ConnectReturnCode::MalformedPacket,
        130 => ConnectReturnCode::ProtocolError,
        131 => ConnectReturnCode::ImplementationSpecificError,
        132 => ConnectReturnCode::UnsupportedProtocolVersion,
        133 => ConnectReturnCode::ClientIdentifierNotValid,
        134 => ConnectReturnCode::BadUserNamePassword,
        135 => ConnectReturnCode::NotAuthorized,
        136 => ConnectReturnCode::ServerUnavailable,
        137 => ConnectReturnCode::ServerBusy,
        138 => ConnectReturnCode::Banned,
        140 => ConnectReturnCode::BadAuthenticationMethod,
        144 => ConnectReturnCode::TopicNameInvalid,
        149 => ConnectReturnCode::PacketTooLarge,
        151 => ConnectReturnCode::QuotaExceeded,
        153 => ConnectReturnCode::PayloadFormatInvalid,
        154 => ConnectReturnCode::RetainNotSupported,
        155 => ConnectReturnCode::QoSNotSupported,
        156 => ConnectReturnCode::UseAnotherServer,
        157 => ConnectReturnCode::ServerMoved,
        159 => ConnectReturnCode::ConnectionRateExceeded,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

fn connect_code(return_code: ConnectReturnCode) -> u8 {
    match return_code {
        ConnectReturnCode::Success => 0,
        ConnectReturnCode::UnspecifiedError => 128,
        ConnectReturnCode::MalformedPacket => 129,
        ConnectReturnCode::ProtocolError => 130,
        ConnectReturnCode::ImplementationSpecificError => 131,
        ConnectReturnCode::UnsupportedProtocolVersion => 132,
        ConnectReturnCode::ClientIdentifierNotValid => 133,
        ConnectReturnCode::BadUserNamePassword => 134,
        ConnectReturnCode::NotAuthorized => 135,
        ConnectReturnCode::ServerUnavailable => 136,
        ConnectReturnCode::ServerBusy => 137,
        ConnectReturnCode::Banned => 138,
        ConnectReturnCode::BadAuthenticationMethod => 140,
        ConnectReturnCode::TopicNameInvalid => 144,
        ConnectReturnCode::PacketTooLarge => 149,
        ConnectReturnCode::QuotaExceeded => 151,
        ConnectReturnCode::PayloadFormatInvalid => 153,
        ConnectReturnCode::RetainNotSupported => 154,
        ConnectReturnCode::QoSNotSupported => 155,
        ConnectReturnCode::UseAnotherServer => 156,
        ConnectReturnCode::ServerMoved => 157,
        ConnectReturnCode::ConnectionRateExceeded => 159,
        _ => unreachable!(),
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
        let connack_props = ConnAckProperties {
            session_expiry_interval: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size: None,
            assigned_client_identifier: None,
            topic_alias_max: None,
            reason_string: None,
            user_properties: vec![(USER_PROP_KEY.into(), USER_PROP_VAL.into())],
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };

        let connack_pkt = ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: Some(connack_props),
        };

        let size_from_size = connack_pkt.size();
        let size_from_write = connack_pkt.write(&mut dummy_bytes).unwrap();
        let size_from_bytes = dummy_bytes.len();

        assert_eq!(size_from_write, size_from_bytes);
        assert_eq!(size_from_size, size_from_bytes);
    }
}

impl From<ConnectReturnCode> for u8 {
    fn from(value: ConnectReturnCode) -> Self {
        connect_code(value)
    }
}
impl TryFrom<u8> for ConnectReturnCode {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        connect_return(value)
    }
}
