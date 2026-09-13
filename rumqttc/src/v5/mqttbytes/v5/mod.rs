use std::slice::Iter;

pub use self::{
    auth::{Auth, AuthProperties, AuthReasonCode},
    codec::Codec,
    connack::{ConnAck, ConnAckProperties, ConnectReturnCode},
    connect::{Connect, ConnectProperties, LastWill, LastWillProperties, Login},
    disconnect::{Disconnect, DisconnectProperties, DisconnectReasonCode},
    ping::{PingReq, PingResp},
    puback::{PubAck, PubAckProperties, PubAckReason},
    pubcomp::{PubComp, PubCompProperties, PubCompReason},
    publish::{Publish, PublishProperties},
    pubrec::{PubRec, PubRecProperties, PubRecReason},
    pubrel::{PubRel, PubRelProperties, PubRelReason},
    suback::{SubAck, SubAckProperties, SubscribeReasonCode},
    subscribe::{Filter, RetainForwardRule, Subscribe, SubscribeProperties},
    unsuback::{UnsubAck, UnsubAckProperties, UnsubAckReason},
    unsubscribe::{Unsubscribe, UnsubscribeProperties},
};

use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

mod auth;
mod codec;
mod connack;
mod connect;
mod disconnect;
mod ping;
mod puback;
mod pubcomp;
mod publish;
mod pubrec;
mod pubrel;
mod suback;
mod subscribe;
mod unsuback;
mod unsubscribe;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Auth(Auth),
    Connect(Connect, Option<LastWill>, Option<Login>),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PingReq(PingReq),
    PingResp(PingResp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    Disconnect(Disconnect),
}

/// Original order of decoded property identifiers. Repeated identifiers occur
/// once per occurrence. Values remain in the corresponding typed property fields.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PropertyOrder {
    pub packet: Vec<u8>,
    pub will: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPacket {
    pub packet: Packet,
    pub property_order: PropertyOrder,
}

impl Packet {
    /// Reads a stream of bytes and extracts next MQTT packet out of it
    pub fn read(stream: &mut BytesMut, max_size: Option<u32>) -> Result<Packet, Error> {
        Self::read_traced(stream, max_size, &mut PropertyOrder::default())
    }

    /// Decode once while retaining property ordering alongside the typed fields.
    pub fn read_with_property_order(
        stream: &mut BytesMut,
        max_size: Option<u32>,
    ) -> Result<DecodedPacket, Error> {
        let mut property_order = PropertyOrder::default();
        let packet = Self::read_traced(stream, max_size, &mut property_order)?;
        Ok(DecodedPacket {
            packet,
            property_order,
        })
    }

    /// Decode exactly one immutable frame. Borrowed fields share this allocation;
    /// callers may retain an owner to reclaim/erase it after dropping the packet.
    pub fn read_frame_with_property_order(
        frame: Bytes,
        max_size: Option<u32>,
    ) -> Result<DecodedPacket, Error> {
        let fixed_header = check(frame.iter(), max_size)?;
        if fixed_header.frame_length() != frame.len() {
            return Err(Error::MalformedPacket);
        }
        let mut property_order = PropertyOrder::default();
        let packet = Self::decode_frame_traced(fixed_header, frame, &mut property_order)?;
        Ok(DecodedPacket {
            packet,
            property_order,
        })
    }

    fn read_traced(
        stream: &mut BytesMut,
        max_size: Option<u32>,
        order: &mut PropertyOrder,
    ) -> Result<Packet, Error> {
        let fixed_header = check(stream.iter(), max_size)?;

        // Test with a stream with exactly the size to check border panics
        let packet = stream.split_to(fixed_header.frame_length()).freeze();
        Self::decode_frame_traced(fixed_header, packet, order)
    }

    fn decode_frame_traced(
        fixed_header: FixedHeader,
        packet: Bytes,
        order: &mut PropertyOrder,
    ) -> Result<Packet, Error> {
        let packet_type = fixed_header.packet_type()?;

        if fixed_header.remaining_len == 0 {
            // no payload packets, Disconnect still has a bit more info
            return match packet_type {
                PacketType::PingReq => Ok(Packet::PingReq(PingReq)),
                PacketType::PingResp => Ok(Packet::PingResp(PingResp)),
                PacketType::Disconnect => Ok(Packet::Disconnect(Disconnect::new(
                    DisconnectReasonCode::NormalDisconnection,
                ))),
                PacketType::Auth => Ok(Packet::Auth(Auth {
                    code: AuthReasonCode::Success,
                    properties: None,
                })),
                _ => Err(Error::PayloadRequired),
            };
        }

        let packet = match packet_type {
            PacketType::Connect => {
                let (connect, will, login) =
                    Connect::read_traced(fixed_header, packet, &mut order.packet, &mut order.will)?;
                Packet::Connect(connect, will, login)
            }
            PacketType::Publish => {
                let publish = Publish::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::Publish(publish)
            }
            PacketType::Subscribe => {
                let subscribe = Subscribe::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::Subscribe(subscribe)
            }
            PacketType::Unsubscribe => {
                let unsubscribe =
                    Unsubscribe::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::Unsubscribe(unsubscribe)
            }
            PacketType::ConnAck => {
                let connack = ConnAck::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::ConnAck(connack)
            }
            PacketType::PubAck => {
                let puback = PubAck::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::PubAck(puback)
            }
            PacketType::PubRec => {
                let pubrec = PubRec::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::PubRec(pubrec)
            }
            PacketType::PubRel => {
                let pubrel = PubRel::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::PubRel(pubrel)
            }
            PacketType::PubComp => {
                let pubcomp = PubComp::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::PubComp(pubcomp)
            }
            PacketType::SubAck => {
                let suback = SubAck::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::SubAck(suback)
            }
            PacketType::UnsubAck => {
                let unsuback = UnsubAck::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::UnsubAck(unsuback)
            }
            PacketType::PingReq | PacketType::PingResp => return Err(Error::MalformedPacket),
            PacketType::Auth => {
                Packet::Auth(Auth::read_traced(fixed_header, packet, &mut order.packet)?)
            }
            PacketType::Disconnect => {
                let disconnect = Disconnect::read_traced(fixed_header, packet, &mut order.packet)?;
                Packet::Disconnect(disconnect)
            }
        };

        Ok(packet)
    }

    pub fn write(&self, write: &mut BytesMut, max_size: Option<u32>) -> Result<usize, Error> {
        if let Some(max_size) = max_size {
            if self.size() > max_size as usize {
                return Err(Error::OutgoingPacketTooLarge {
                    pkt_size: self.size() as u32,
                    max: max_size,
                });
            }
        }

        match self {
            Self::Auth(auth) => auth.write(write),
            Self::Publish(publish) => publish.write(write),
            Self::Subscribe(subscription) => subscription.write(write),
            Self::Unsubscribe(unsubscribe) => unsubscribe.write(write),
            Self::ConnAck(ack) => ack.write(write),
            Self::PubAck(ack) => ack.write(write),
            Self::SubAck(ack) => ack.write(write),
            Self::UnsubAck(unsuback) => unsuback.write(write),
            Self::PubRec(pubrec) => pubrec.write(write),
            Self::PubRel(pubrel) => pubrel.write(write),
            Self::PubComp(pubcomp) => pubcomp.write(write),
            Self::Connect(connect, will, login) => connect.write(will, login, write),
            Self::PingReq(_) => PingReq::write(write),
            Self::PingResp(_) => PingResp::write(write),
            Self::Disconnect(disconnect) => disconnect.write(write),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Auth(auth) => auth.size(),
            Self::Publish(publish) => publish.size(),
            Self::Subscribe(subscription) => subscription.size(),
            Self::Unsubscribe(unsubscribe) => unsubscribe.size(),
            Self::ConnAck(ack) => ack.size(),
            Self::PubAck(ack) => ack.size(),
            Self::SubAck(ack) => ack.size(),
            Self::UnsubAck(unsuback) => unsuback.size(),
            Self::PubRec(pubrec) => pubrec.size(),
            Self::PubRel(pubrel) => pubrel.size(),
            Self::PubComp(pubcomp) => pubcomp.size(),
            Self::Connect(connect, will, login) => connect.size(will, login),
            Self::PingReq(req) => req.size(),
            Self::PingResp(resp) => resp.size(),
            Self::Disconnect(disconnect) => disconnect.size(),
        }
    }
}

/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349207>
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    remaining_len: usize,
}

impl FixedHeader {
    /// Parse and validate a fixed header without waiting for its payload.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let header = parse_fixed_header(bytes.iter())?;
        header.packet_type()?;
        Ok(header)
    }

    pub fn header_length(&self) -> usize {
        self.fixed_header_len
    }
    pub fn remaining_length(&self) -> usize {
        self.remaining_len
    }

    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_header_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
        let flags = self.byte1 & 15;
        if num == 3 {
            if flags & 6 == 6 || flags & 6 == 0 && flags & 8 != 0 {
                return Err(Error::MalformedPacket);
            }
        } else if flags != if matches!(num, 6 | 8 | 10) { 2 } else { 0 } {
            return Err(Error::MalformedPacket);
        }
        match num {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PubAck),
            5 => Ok(PacketType::PubRec),
            6 => Ok(PacketType::PubRel),
            7 => Ok(PacketType::PubComp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubAck),
            12 => Ok(PacketType::PingReq),
            13 => Ok(PacketType::PingResp),
            14 => Ok(PacketType::Disconnect),
            15 => Ok(PacketType::Auth),
            _ => Err(Error::InvalidPacketType(num)),
        }
    }

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_header_len + self.remaining_len
    }
}

// Isolate the declared property section before decoding any field. A malformed
// field cannot consume bytes from a publish payload, client id or subscribe list.
fn read_properties_section(bytes: &mut Bytes) -> Result<Bytes, Error> {
    let (prefix, size) = length(bytes.iter())?;
    if bytes.remaining() < prefix + size {
        return Err(Error::MalformedPacket);
    }
    bytes.advance(prefix);
    Ok(bytes.split_to(size))
}

fn validate_property_occurrence(
    id: u8,
    repeat: bool,
    seen: &mut u64,
    count: &mut usize,
) -> Result<(), Error> {
    if id >= 64 {
        return Err(Error::InvalidPropertyType(id));
    }
    *count += 1;
    if *count > 1024 || (!repeat && *seen & (1u64 << id) != 0) {
        return Err(Error::MalformedPacket);
    }
    *seen |= 1u64 << id;
    Ok(())
}

fn property(num: u8) -> Result<PropertyType, Error> {
    let property = match num {
        1 => PropertyType::PayloadFormatIndicator,
        2 => PropertyType::MessageExpiryInterval,
        3 => PropertyType::ContentType,
        8 => PropertyType::ResponseTopic,
        9 => PropertyType::CorrelationData,
        11 => PropertyType::SubscriptionIdentifier,
        17 => PropertyType::SessionExpiryInterval,
        18 => PropertyType::AssignedClientIdentifier,
        19 => PropertyType::ServerKeepAlive,
        21 => PropertyType::AuthenticationMethod,
        22 => PropertyType::AuthenticationData,
        23 => PropertyType::RequestProblemInformation,
        24 => PropertyType::WillDelayInterval,
        25 => PropertyType::RequestResponseInformation,
        26 => PropertyType::ResponseInformation,
        28 => PropertyType::ServerReference,
        31 => PropertyType::ReasonString,
        33 => PropertyType::ReceiveMaximum,
        34 => PropertyType::TopicAliasMaximum,
        35 => PropertyType::TopicAlias,
        36 => PropertyType::MaximumQos,
        37 => PropertyType::RetainAvailable,
        38 => PropertyType::UserProperty,
        39 => PropertyType::MaximumPacketSize,
        40 => PropertyType::WildcardSubscriptionAvailable,
        41 => PropertyType::SubscriptionIdentifierAvailable,
        42 => PropertyType::SharedSubscriptionAvailable,
        num => return Err(Error::InvalidPropertyType(num)),
    };

    Ok(property)
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
pub fn check(stream: Iter<u8>, max_packet_size: Option<u32>) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if let Some(max_size) = max_packet_size {
        if fixed_header.remaining_len > max_size as usize {
            return Err(Error::PayloadSizeLimitExceeded {
                pkt_size: fixed_header.remaining_len,
                max: max_size,
            });
        }
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header
fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = length(stream)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            if len_len > 1 && byte == 0 {
                return Err(Error::MalformedRemainingLength);
            }
            break;
        }

        shift += 7;

        // Only a max of 4 bytes allowed for remaining length
        // more than 4 shifts (0, 7, 14, 21) implies bad length
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // Not enough bytes to frame remaining length. wait for
    // one more byte
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((len_len, len))
}

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes(stream: &mut Bytes) -> Result<Bytes, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.len() {
        return Err(Error::BoundaryCrossed(len));
    }

    Ok(stream.split_to(len))
}

/// Reads a string from bytes stream
fn read_mqtt_string(stream: &mut Bytes) -> Result<String, Error> {
    let s = read_mqtt_bytes(stream)?;
    match String::from_utf8(s.to_vec()) {
        Ok(v) if !v.contains('\0') => Ok(v),
        Ok(_) => Err(Error::MalformedPacket),
        Err(_e) => Err(Error::TopicNotUtf8),
    }
}

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut BytesMut, bytes: &[u8]) {
    stream.put_u16(bytes.len() as u16);
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;
    let mut count = 0;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

/// Return number of remaining length bytes required for encoding length
fn len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes
fn read_u16(stream: &mut Bytes) -> Result<u16, Error> {
    if stream.len() < 2 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u16())
}

fn read_u8(stream: &mut Bytes) -> Result<u8, Error> {
    if stream.is_empty() {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u8())
}

fn read_u32(stream: &mut Bytes) -> Result<u32, Error> {
    if stream.len() < 4 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u32())
}

mod test {
    // These are used in tests by packets
    #[allow(dead_code)]
    pub const USER_PROP_KEY: &str = "property";
    #[allow(dead_code)]
    pub const USER_PROP_VAL: &str = "a value thats really long............................................................................................................";
}
