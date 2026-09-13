use super::mqttbytes::v5::{
    ConnAck, ConnectReturnCode, Disconnect, DisconnectReasonCode, Packet, PingReq, PubAck,
    PubAckReason, PubComp, PubCompReason, PubRec, PubRel, Publish, SubAck, Subscribe,
    SubscribeReasonCode, UnsubAck, Unsubscribe,
};
use super::mqttbytes::{self, Error as MqttError, QoS};
use std::sync::{
    atomic::{AtomicBool as RuntimeAtomicBool, Ordering as RuntimeOrdering},
    Arc as RuntimeArc,
};

use super::{Event, Incoming, Outgoing, Request};

use bytes::Bytes;
use fixedbitset::FixedBitSet;
use std::collections::{HashMap, VecDeque};
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    #[error("Conversion error {0:?}")]
    Coversion(#[from] core::num::TryFromIntError),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// No send quota or unused packet identifier remains.
    #[error("MQTT outgoing capacity is occupied")]
    OutgoingCapacity,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid: {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("A Subscribe packet must contain atleast one filter")]
    EmptySubscription,
    #[error("Mqtt serialization/deserialization error: {0}")]
    Deserialization(MqttError),
    #[error(
        "Cannot use topic alias '{alias:?}'. It's greater than the broker's maximum of '{max:?}'."
    )]
    InvalidAlias { alias: u16, max: u16 },
    #[error("Cannot send packet of size '{pkt_size:?}'. It's greater than the broker's maximum packet size of: '{max:?}'")]
    OutgoingPacketTooLarge { pkt_size: u32, max: u32 },
    #[error("Cannot receive packet of size '{pkt_size:?}'. It's greater than the client's maximum packet size of: '{max:?}'")]
    IncomingPacketTooLarge { pkt_size: usize, max: usize },
    #[error("Server sent disconnect with reason `{reason_string:?}` and code '{reason_code:?}' ")]
    ServerDisconnect {
        reason_code: DisconnectReasonCode,
        reason_string: Option<String>,
    },
    #[error("Connection failed with reason '{reason:?}' ")]
    ConnFail { reason: ConnectReturnCode },
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
}

impl From<mqttbytes::Error> for StateError {
    fn from(value: MqttError) -> Self {
        match value {
            MqttError::OutgoingPacketTooLarge { pkt_size, max } => {
                StateError::OutgoingPacketTooLarge { pkt_size, max }
            }
            e => StateError::Deserialization(e),
        }
    }
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
pub struct MqttState {
    transmission_flags: std::collections::BTreeMap<u16, RuntimeArc<RuntimeAtomicBool>>,
    /// Status of last ping
    pub await_pingresp: bool,
    /// Collision ping count. Collisions stop user requests
    /// which inturn trigger pings. Multiple pings without
    /// resolving collisions will result in error
    pub collision_ping_count: usize,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: FixedBitSet,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: FixedBitSet,
    /// Last collision due to broker not acking in order
    pub collision: Option<Publish>,
    /// Buffered incoming packets
    pub events: VecDeque<Event>,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
    /// Map of alias_id->topic
    topic_alises: HashMap<u16, Bytes>,
    /// `topic_alias_maximum` RECEIVED via connack packet
    pub broker_topic_alias_max: u16,
    /// Maximum number of allowed inflight QoS1 & QoS2 requests
    pub(crate) max_outgoing_inflight: u16,
    /// Upper limit on the maximum number of allowed inflight QoS1 & QoS2 requests
    max_outgoing_inflight_upper_limit: u16,
    incoming_ack: FixedBitSet,
    incoming_rec: FixedBitSet,
    incoming_done: HashMap<usize, u64>,
    next_completion: u64,
    outgoing_sub: HashMap<u16, Vec<QoS>>,
    outgoing_unsub: HashMap<u16, usize>,
    outgoing_aliases: HashMap<u16, Bytes>,
    receive_maximum: usize,
    receive_alias_maximum: u16,
    alias_bytes_maximum: usize,
    defer_write_completion: bool,
    resume_queue: VecDeque<u16>,
    resume_sent: FixedBitSet,
    outgoing_expiry_started: HashMap<u16, Instant>,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16, manual_acks: bool) -> Self {
        MqttState {
            transmission_flags: Default::default(),
            await_pingresp: false,
            collision_ping_count: 0,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            inflight: 0,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: FixedBitSet::with_capacity(max_inflight as usize + 1),
            incoming_pub: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            collision: None,
            // TODO: Optimize these sizes later
            events: VecDeque::with_capacity(100),
            manual_acks,
            topic_alises: HashMap::new(),
            // Set via CONNACK
            broker_topic_alias_max: 0,
            max_outgoing_inflight: max_inflight,
            max_outgoing_inflight_upper_limit: max_inflight,
            incoming_ack: FixedBitSet::with_capacity(65536),
            incoming_rec: FixedBitSet::with_capacity(65536),
            incoming_done: HashMap::new(),
            next_completion: 0,
            outgoing_sub: HashMap::new(),
            outgoing_unsub: HashMap::new(),
            outgoing_aliases: HashMap::new(),
            receive_maximum: 65535,
            receive_alias_maximum: 0,
            alias_bytes_maximum: 16 * 1024 * 1024,
            defer_write_completion: false,
            resume_queue: VecDeque::new(),
            resume_sent: FixedBitSet::with_capacity(max_inflight as usize + 1),
            outgoing_expiry_started: HashMap::new(),
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Request> {
        self.transmission_flags.clear();
        let mut pending = Vec::with_capacity(100);
        // remove and collect pending publishes
        for publish in self.outgoing_pub.iter_mut() {
            if let Some(publish) = publish.take() {
                let request = Request::Publish(publish);
                pending.push(request);
            }
        }

        // remove and collect pending releases
        for pkid in self.outgoing_rel.ones() {
            let request = Request::PubRel(PubRel::new(pkid as u16, None));
            pending.push(request);
        }
        self.outgoing_rel.clear();

        // remove packed ids of incoming qos2 publishes
        self.incoming_pub.clear();
        self.incoming_ack.clear();
        self.incoming_rec.clear();
        self.incoming_done.clear();
        self.outgoing_sub.clear();
        self.outgoing_unsub.clear();
        self.topic_alises.clear();
        self.outgoing_aliases.clear();
        self.resume_queue.clear();
        self.resume_sent.clear();
        self.outgoing_expiry_started.clear();

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.inflight = 0;
        pending
    }

    /// Begin recovery of the same verified broker session. Alias and quota
    /// negotiation is connection-local; QoS receive/replay state is not cleared.
    /// Track whether a controlled writer may have transmitted a publication.
    /// A cancelled request that never entered the writer must not be replayed.
    pub fn track_publish_transmission(&mut self, id: u16, flag: RuntimeArc<RuntimeAtomicBool>) {
        self.transmission_flags.insert(id, flag);
    }
    fn discard_unsent_publications(&mut self) {
        let unsent: Vec<_> = self
            .transmission_flags
            .iter()
            .filter_map(|(id, flag)| (!flag.load(RuntimeOrdering::Acquire)).then_some(*id))
            .collect();
        for id in unsent {
            if self
                .outgoing_pub
                .get_mut(id as usize)
                .and_then(Option::take)
                .is_some()
            {
                self.inflight -= 1;
            }
            self.transmission_flags.remove(&id);
        }
        self.transmission_flags.retain(|id, _| {
            self.outgoing_pub
                .get(*id as usize)
                .is_some_and(Option::is_some)
        });
    }
    pub fn resume_session(&mut self) -> Vec<u16> {
        self.discard_unsent_publications();
        self.outgoing_expiry_started.retain(|id, _| {
            self.outgoing_pub
                .get(*id as usize)
                .is_some_and(Option::is_some)
        });
        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.last_incoming = Instant::now();
        self.last_outgoing = Instant::now();
        self.events.clear();
        self.topic_alises.clear();
        self.outgoing_aliases.clear();
        self.broker_topic_alias_max = 0;
        self.max_outgoing_inflight = self.max_outgoing_inflight_upper_limit;
        self.resume_sent.clear();
        self.resume_queue.clear();
        // PUBREL is not constrained by Receive Maximum.
        self.resume_queue
            .extend(self.outgoing_rel.ones().map(|id| id as u16));
        self.resume_queue
            .extend(self.outgoing_pub.iter().flatten().map(|p| p.pkid));
        let interrupted = self
            .outgoing_sub
            .keys()
            .chain(self.outgoing_unsub.keys())
            .copied()
            .collect();
        self.outgoing_sub.clear();
        self.outgoing_unsub.clear();
        interrupted
    }
    pub fn has_resumed_packet(&self) -> bool {
        self.resume_queue.front().is_some_and(|id| {
            self.outgoing_rel.contains(*id as usize)
                || self.resume_sent.count_ones(..) < self.max_outgoing_inflight as usize
        })
    }
    /// Emits at most one retransmission within this connection's negotiated
    /// Receive Maximum. Call after acknowledgements and while output has room.
    pub fn next_resumed_packet(&mut self) -> Option<Packet> {
        while self.has_resumed_packet() {
            let id = self.resume_queue.pop_front()?;
            if self.outgoing_rel.contains(id as usize) {
                return Some(Packet::PubRel(PubRel::new(id, None)));
            }
            let Some(mut publish) = self.outgoing_pub[id as usize].clone() else {
                continue;
            };
            if let Some(properties) = &mut publish.properties {
                properties.topic_alias = None;
                if let Some(expiry) = &mut properties.message_expiry_interval {
                    let elapsed = self
                        .outgoing_expiry_started
                        .get(&id)
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0);
                    if elapsed >= u64::from(*expiry) {
                        self.outgoing_pub[id as usize] = None;
                        self.outgoing_expiry_started.remove(&id);
                        self.inflight -= 1;
                        continue;
                    }
                    *expiry -= elapsed as u32;
                }
            }
            publish.dup = true;
            self.resume_sent.insert(id as usize);
            return Some(Packet::Publish(publish));
        }
        None
    }

    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(
        &mut self,
        request: Request,
    ) -> Result<Option<Packet>, StateError> {
        let packet = match request {
            Request::Publish(publish) => self.outgoing_publish(publish)?,
            Request::PubRel(pubrel) => self.outgoing_pubrel(pubrel)?,
            Request::Subscribe(subscribe) => self.outgoing_subscribe(subscribe)?,
            Request::Unsubscribe(unsubscribe) => self.outgoing_unsubscribe(unsubscribe)?,
            Request::PingReq => self.outgoing_ping()?,
            Request::Disconnect => {
                self.outgoing_disconnect(DisconnectReasonCode::NormalDisconnection)?
            }
            Request::PubAck(puback) => self.outgoing_puback(puback)?,
            Request::PubRec(pubrec) => self.outgoing_pubrec(pubrec)?,
            _ => return Err(StateError::WrongPacket),
        };

        self.last_outgoing = Instant::now();
        Ok(packet)
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(
        &mut self,
        mut packet: Incoming,
    ) -> Result<Option<Packet>, StateError> {
        let duplicate = matches!(&packet, Incoming::Publish(p) if p.qos == QoS::ExactlyOnce && self.incoming_pub.contains(p.pkid as usize));
        let event_index = self.events.len();

        let outgoing = match &mut packet {
            Incoming::PingResp(_) => self.handle_incoming_pingresp()?,
            Incoming::Publish(publish) => self.handle_incoming_publish(publish)?,
            Incoming::SubAck(suback) => self.handle_incoming_suback(suback)?,
            Incoming::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback)?,
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback)?,
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec)?,
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel)?,
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp)?,
            Incoming::ConnAck(connack) => self.handle_incoming_connack(connack)?,
            Incoming::Disconnect(disconn) => self.handle_incoming_disconn(disconn)?,
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };
        match &packet {
            Incoming::PubAck(p) => {
                self.resume_sent.set(p.pkid as usize, false);
                self.outgoing_expiry_started.remove(&p.pkid);
            }
            Incoming::PubComp(p) => {
                self.resume_sent.set(p.pkid as usize, false);
                self.outgoing_expiry_started.remove(&p.pkid);
            }
            Incoming::PubRec(p) if u8::from(p.reason) >= 128 => {
                self.resume_sent.set(p.pkid as usize, false);
                self.outgoing_expiry_started.remove(&p.pkid);
            }
            _ => {}
        }

        if !duplicate {
            self.events.insert(event_index, Event::Incoming(packet));
        }
        self.transmission_flags.retain(|id, _| {
            self.outgoing_pub
                .get(*id as usize)
                .is_some_and(Option::is_some)
        });
        self.last_incoming = Instant::now();
        Ok(outgoing)
    }

    pub fn handle_protocol_error(&mut self) -> Result<Option<Packet>, StateError> {
        // send DISCONNECT packet with REASON_CODE 0x82
        self.outgoing_disconnect(DisconnectReasonCode::ProtocolError)
    }

    fn handle_incoming_suback(
        &mut self,
        suback: &mut SubAck,
    ) -> Result<Option<Packet>, StateError> {
        let requested = self
            .outgoing_sub
            .get(&suback.pkid)
            .ok_or(StateError::Unsolicited(suback.pkid))?;
        if requested.len() != suback.return_codes.len()
            || requested
                .iter()
                .zip(&suback.return_codes)
                .any(|(q, c)| matches!(c, SubscribeReasonCode::Success(v) if *v as u8 > *q as u8))
        {
            return Err(StateError::WrongPacket);
        }
        self.outgoing_sub.remove(&suback.pkid);
        Ok(None)
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: &mut UnsubAck,
    ) -> Result<Option<Packet>, StateError> {
        if self.outgoing_unsub.get(&unsuback.pkid).copied() != Some(unsuback.reasons.len()) {
            return Err(StateError::Unsolicited(unsuback.pkid));
        }
        self.outgoing_unsub.remove(&unsuback.pkid);
        Ok(None)
    }

    fn handle_incoming_connack(
        &mut self,
        connack: &mut ConnAck,
    ) -> Result<Option<Packet>, StateError> {
        if connack.code != ConnectReturnCode::Success {
            return Err(StateError::ConnFail {
                reason: connack.code,
            });
        }

        if let Some(props) = &connack.properties {
            if let Some(topic_alias_max) = props.topic_alias_max {
                self.broker_topic_alias_max = topic_alias_max
            }

            if let Some(max_inflight) = props.receive_max {
                self.max_outgoing_inflight =
                    max_inflight.min(self.max_outgoing_inflight_upper_limit);
                // FIXME: Maybe resize the pubrec and pubrel queues here
                // to save some space.
            }
        }
        Ok(None)
    }

    fn handle_incoming_disconn(
        &mut self,
        disconn: &mut Disconnect,
    ) -> Result<Option<Packet>, StateError> {
        let reason_code = disconn.reason_code;
        let reason_string = if let Some(props) = &disconn.properties {
            props.reason_string.clone()
        } else {
            None
        };
        Err(StateError::ServerDisconnect {
            reason_code,
            reason_string,
        })
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(
        &mut self,
        publish: &mut Publish,
    ) -> Result<Option<Packet>, StateError> {
        let id = publish.pkid as usize;
        let alias = publish.properties.as_ref().and_then(|p| p.topic_alias);
        if let Some(alias) = alias {
            if alias == 0 || alias > self.receive_alias_maximum {
                return Err(StateError::InvalidAlias {
                    alias,
                    max: self.receive_alias_maximum,
                });
            }
            if publish.topic.is_empty() {
                publish.topic = self
                    .topic_alises
                    .get(&alias)
                    .cloned()
                    .ok_or(StateError::InvalidState)?;
            } else {
                Self::insert_alias(
                    &mut self.topic_alises,
                    alias,
                    Bytes::copy_from_slice(&publish.topic),
                    self.alias_bytes_maximum,
                )?;
            }
        }
        if publish.topic.is_empty() || std::str::from_utf8(&publish.topic).is_err() {
            return Err(StateError::WrongPacket);
        }
        if publish.qos == QoS::AtMostOnce {
            return Ok(None);
        }
        if id == 0 {
            return Err(StateError::WrongPacket);
        }
        let known = self.incoming_pub.contains(id) || self.incoming_ack.contains(id);
        if !known && self.incoming_inflight() >= self.receive_maximum {
            return Err(StateError::InvalidState);
        }
        match publish.qos {
            QoS::AtMostOnce => Ok(None),
            QoS::AtLeastOnce => {
                if self.incoming_pub.contains(id)
                    || (self.incoming_ack.contains(id) && !publish.dup)
                {
                    return Err(StateError::WrongPacket);
                }
                self.incoming_ack.insert(id);
                if !self.manual_acks && !self.incoming_done.contains_key(&id) {
                    return self.outgoing_puback(PubAck::new(publish.pkid, None));
                }
                Ok(None)
            }
            QoS::ExactlyOnce => {
                if self.incoming_ack.contains(id) {
                    return Err(StateError::WrongPacket);
                }
                if self.incoming_pub.contains(id) {
                    if !publish.dup || self.incoming_done.contains_key(&id) {
                        return Err(StateError::WrongPacket);
                    }
                    if self.incoming_rec.contains(id) {
                        return Ok(None);
                    }
                    return self.outgoing_pubrec(PubRec::new(publish.pkid, None));
                }
                self.incoming_pub.insert(id);
                self.incoming_rec.insert(id);
                if !self.manual_acks {
                    return self.outgoing_pubrec(PubRec::new(publish.pkid, None));
                }
                Ok(None)
            }
        }
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<Option<Packet>, StateError> {
        let publish = self
            .outgoing_pub
            .get_mut(puback.pkid as usize)
            .ok_or(StateError::Unsolicited(puback.pkid))?;

        if !publish.as_ref().is_some_and(|p| p.qos == QoS::AtLeastOnce) {
            error!("Unsolicited puback packet: {:?}", puback.pkid);
            return Err(StateError::Unsolicited(puback.pkid));
        }

        publish.take();
        self.inflight -= 1;

        if puback.reason != PubAckReason::Success
            && puback.reason != PubAckReason::NoMatchingSubscribers
        {
            warn!(
                "PubAck Pkid = {:?}, reason: {:?}",
                puback.pkid, puback.reason
            );
            return Ok(None);
        }

        if let Some(publish) = self.check_collision(puback.pkid) {
            self.outgoing_pub[publish.pkid as usize] = Some(publish.clone());
            self.inflight += 1;

            let pkid = publish.pkid;
            let event = Event::Outgoing(Outgoing::Publish(pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            return Ok(Some(Packet::Publish(publish)));
        }

        Ok(None)
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
        let id = pubrec.pkid as usize;
        let reason = u8::from(pubrec.reason);
        if self.outgoing_rel.contains(id) {
            if reason >= 128 {
                return Err(StateError::WrongPacket);
            }
            return Ok(Some(Packet::PubRel(PubRel::new(pubrec.pkid, None))));
        }
        let slot = self
            .outgoing_pub
            .get_mut(id)
            .ok_or(StateError::Unsolicited(pubrec.pkid))?;
        if !slot.as_ref().is_some_and(|p| p.qos == QoS::ExactlyOnce) {
            return Err(StateError::Unsolicited(pubrec.pkid));
        }
        slot.take();
        if reason >= 128 {
            self.inflight -= 1;
            return Ok(None);
        }
        self.outgoing_rel.insert(id);
        self.events
            .push_back(Event::Outgoing(Outgoing::PubRel(pubrec.pkid)));
        Ok(Some(Packet::PubRel(PubRel::new(pubrec.pkid, None))))
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
        let id = pubrel.pkid as usize;
        if id == 0 || self.incoming_rec.contains(id) || self.incoming_ack.contains(id) {
            return Err(StateError::WrongPacket);
        }
        let known = self.incoming_pub.contains(id);
        if known {
            let token = self.mark_completing(id)?;
            if !self.defer_write_completion {
                self.acknowledgement_written(token);
            }
        }
        let mut packet = PubComp::new(pubrel.pkid, None);
        if !known {
            packet.reason = PubCompReason::PacketIdentifierNotFound;
        }
        self.events
            .push_back(Event::Outgoing(Outgoing::PubComp(pubrel.pkid)));
        Ok(Some(Packet::PubComp(packet)))
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
        if !self.outgoing_rel.contains(pubcomp.pkid as usize) {
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }
        let outgoing = self.check_collision(pubcomp.pkid).map(|publish| {
            let pkid = publish.pkid;
            let event = Event::Outgoing(Outgoing::Publish(pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            Packet::Publish(publish)
        });

        if !self.outgoing_rel.contains(pubcomp.pkid as usize) {
            error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }
        self.outgoing_rel.set(pubcomp.pkid as usize, false);

        self.inflight -= 1;
        if pubcomp.reason != PubCompReason::Success {
            warn!(
                "PubComp Pkid = {:?}, reason: {:?}",
                pubcomp.pkid, pubcomp.reason
            );
            return Ok(None);
        }

        Ok(outgoing)
    }

    fn handle_incoming_pingresp(&mut self) -> Result<Option<Packet>, StateError> {
        if !self.await_pingresp {
            return Err(StateError::WrongPacket);
        }
        self.await_pingresp = false;
        Ok(None)
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(&mut self, mut publish: Publish) -> Result<Option<Packet>, StateError> {
        if let Some(alias) = publish.properties.as_ref().and_then(|p| p.topic_alias) {
            if alias == 0 || alias > self.broker_topic_alias_max {
                return Err(StateError::InvalidAlias {
                    alias,
                    max: self.broker_topic_alias_max,
                });
            }
            if publish.topic.is_empty() && !self.outgoing_aliases.contains_key(&alias) {
                return Err(StateError::InvalidState);
            }
        }
        if let Some(alias) = publish.properties.as_ref().and_then(|p| p.topic_alias) {
            if !publish.topic.is_empty() {
                Self::check_alias(
                    &self.outgoing_aliases,
                    alias,
                    &publish.topic,
                    self.alias_bytes_maximum,
                )?;
            }
        }
        if publish.qos != QoS::AtMostOnce && self.inflight >= self.max_outgoing_inflight {
            return Err(StateError::OutgoingCapacity);
        }

        if publish.qos != QoS::AtMostOnce {
            if publish.pkid == 0 {
                publish.pkid = self.next_available_packet_id()?;
            }

            let pkid = publish.pkid;
            if self.outgoing_rel.contains(pkid as usize)
                || self.outgoing_sub.contains_key(&pkid)
                || self.outgoing_unsub.contains_key(&pkid)
            {
                return Err(StateError::InvalidState);
            }
            if self
                .outgoing_pub
                .get(publish.pkid as usize)
                .ok_or(StateError::Unsolicited(publish.pkid))?
                .is_some()
            {
                info!("Collision on packet id = {:?}", publish.pkid);
                self.collision = Some(publish);
                let event = Event::Outgoing(Outgoing::AwaitAck(pkid));
                self.events.push_back(event);
                return Ok(None);
            }

            // if there is an existing publish at this pkid, this implies that broker hasn't acked this
            // packet yet. This error is possible only when broker isn't acking sequentially
            let mut retained = publish.clone();
            if retained.topic.is_empty() {
                let alias = retained
                    .properties
                    .as_ref()
                    .and_then(|p| p.topic_alias)
                    .ok_or(StateError::InvalidState)?;
                retained.topic = self
                    .outgoing_aliases
                    .get(&alias)
                    .ok_or(StateError::InvalidState)?
                    .clone();
            }
            if retained
                .properties
                .as_ref()
                .and_then(|p| p.message_expiry_interval)
                .is_some()
            {
                self.outgoing_expiry_started.insert(pkid, Instant::now());
            }
            self.outgoing_pub[pkid as usize] = Some(retained);
            self.inflight += 1;
        };

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            String::from_utf8_lossy(&publish.topic),
            publish.pkid,
            publish.payload.len()
        );

        let pkid = publish.pkid;

        if let Some(alias) = publish.properties.as_ref().and_then(|p| p.topic_alias) {
            if !publish.topic.is_empty() {
                self.outgoing_aliases
                    .insert(alias, Bytes::copy_from_slice(&publish.topic));
            }
        }

        let event = Event::Outgoing(Outgoing::Publish(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Publish(publish)))
    }

    fn outgoing_pubrel(&mut self, pubrel: PubRel) -> Result<Option<Packet>, StateError> {
        let pubrel = self.save_pubrel(pubrel)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);

        let event = Event::Outgoing(Outgoing::PubRel(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(PubRel::new(pubrel.pkid, None))))
    }

    fn outgoing_puback(&mut self, puback: PubAck) -> Result<Option<Packet>, StateError> {
        let id = puback.pkid as usize;
        if id == 0 || !self.incoming_ack.contains(id) || self.incoming_done.contains_key(&id) {
            return Err(StateError::Unsolicited(puback.pkid));
        }
        let token = self.mark_completing(id)?;
        if !self.defer_write_completion {
            self.acknowledgement_written(token);
        }
        self.events
            .push_back(Event::Outgoing(Outgoing::PubAck(puback.pkid)));
        Ok(Some(Packet::PubAck(puback)))
    }

    fn outgoing_pubrec(&mut self, pubrec: PubRec) -> Result<Option<Packet>, StateError> {
        let id = pubrec.pkid as usize;
        if id == 0 || !self.incoming_pub.contains(id) || self.incoming_done.contains_key(&id) {
            return Err(StateError::Unsolicited(pubrec.pkid));
        }
        self.incoming_rec.set(id, false);
        if u8::from(pubrec.reason) >= 128 {
            let token = self.mark_completing(id)?;
            if !self.defer_write_completion {
                self.acknowledgement_written(token);
            }
        }
        self.events
            .push_back(Event::Outgoing(Outgoing::PubRec(pubrec.pkid)));
        Ok(Some(Packet::PubRec(pubrec)))
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<Option<Packet>, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        if self.collision.is_some() {
            self.collision_ping_count += 1;
            if self.collision_ping_count >= 2 {
                return Err(StateError::CollisionTimeout);
            }
        }

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq, last incoming packet before {:?}, last outgoing request before {:?}",
            elapsed_in, elapsed_out,
        );

        let event = Event::Outgoing(Outgoing::PingReq);
        self.events.push_back(event);

        Ok(Some(Packet::PingReq(PingReq)))
    }

    fn outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
    ) -> Result<Option<Packet>, StateError> {
        if subscription.filters.is_empty() {
            return Err(StateError::EmptySubscription);
        }
        let id = self.next_available_packet_id()?;
        subscription.pkid = id;
        self.outgoing_sub
            .insert(id, subscription.filters.iter().map(|f| f.qos).collect());
        self.events
            .push_back(Event::Outgoing(Outgoing::Subscribe(id)));
        Ok(Some(Packet::Subscribe(subscription)))
    }

    fn outgoing_unsubscribe(
        &mut self,
        mut unsub: Unsubscribe,
    ) -> Result<Option<Packet>, StateError> {
        if unsub.filters.is_empty() {
            return Err(StateError::EmptySubscription);
        }
        let id = self.next_available_packet_id()?;
        unsub.pkid = id;
        self.outgoing_unsub.insert(id, unsub.filters.len());
        self.events
            .push_back(Event::Outgoing(Outgoing::Unsubscribe(id)));
        Ok(Some(Packet::Unsubscribe(unsub)))
    }

    fn outgoing_disconnect(
        &mut self,
        reason: DisconnectReasonCode,
    ) -> Result<Option<Packet>, StateError> {
        debug!("Disconnect with {:?}", reason);
        let event = Event::Outgoing(Outgoing::Disconnect);
        self.events.push_back(event);

        Ok(Some(Packet::Disconnect(Disconnect::new(reason))))
    }

    fn check_collision(&mut self, pkid: u16) -> Option<Publish> {
        if let Some(publish) = &self.collision {
            if publish.pkid == pkid {
                return self.collision.take();
            }
        }

        None
    }

    fn save_pubrel(&mut self, mut pubrel: PubRel) -> Result<PubRel, StateError> {
        let pubrel = match pubrel.pkid {
            // consider PacketIdentifier(0) as uninitialized packets
            0 => {
                pubrel.pkid = self.next_pkid();
                pubrel
            }
            _ => pubrel,
        };

        self.outgoing_rel.insert(pubrel.pkid as usize);
        self.inflight += 1;
        Ok(pubrel)
    }

    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    /// Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    ///
    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;

        // When next packet id is at the edge of inflight queue,
        // set await flag. This instructs eventloop to stop
        // processing requests until all the inflight publishes
        // are acked
        if next_pkid == self.max_outgoing_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

impl MqttState {
    pub fn initial_memory_bound(maximum: u16) -> usize {
        let slots = maximum as usize + 1;
        std::mem::size_of::<Self>()
            + (slots + 7) / 8
            + 64
            + slots * std::mem::size_of::<Option<Publish>>()
            + (slots + 7) / 8
            + 256
            + 3 * (8192 + 64)
            + 100 * std::mem::size_of::<Event>()
    }
    pub fn pending(&self) -> bool {
        self.inflight != 0 || !self.outgoing_sub.is_empty() || !self.outgoing_unsub.is_empty()
    }
    pub fn incoming_inflight(&self) -> usize {
        self.incoming_ack.count_ones(..) + self.incoming_pub.count_ones(..)
    }
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.transmission_flags.len() * 128
            + self.resume_queue.capacity() * std::mem::size_of::<u16>()
            + std::mem::size_of_val(self.resume_sent.as_slice())
            + self.outgoing_expiry_started.capacity() * (64 + std::mem::size_of::<Instant>())
            + self.outgoing_pub.capacity() * std::mem::size_of::<Option<Publish>>()
            + self
                .outgoing_pub
                .iter()
                .flatten()
                .map(Self::publication_memory_bound)
                .sum::<usize>()
            + [
                &self.outgoing_rel,
                &self.incoming_pub,
                &self.incoming_ack,
                &self.incoming_rec,
            ]
            .iter()
            .map(|s| std::mem::size_of_val(s.as_slice()))
            .sum::<usize>()
            + self
                .outgoing_sub
                .values()
                .map(|v| v.capacity() * std::mem::size_of::<QoS>())
                .sum::<usize>()
            + (self.outgoing_sub.capacity()
                + self.outgoing_unsub.capacity()
                + self.incoming_done.capacity()
                + self.topic_alises.capacity()
                + self.outgoing_aliases.capacity())
                * 128
            + self
                .topic_alises
                .values()
                .chain(self.outgoing_aliases.values())
                .map(|v| v.len())
                .sum::<usize>()
            + self.events.capacity() * std::mem::size_of::<Event>()
    }
    /// Conservative owned heap estimate; property containers can exceed wire size.
    pub fn publication_memory_bound(p: &Publish) -> usize {
        p.topic.len()
            + p.payload.len()
            + p.properties.as_ref().map_or(0, |v| {
                v.response_topic.as_ref().map_or(0, String::capacity)
                    + v.content_type.as_ref().map_or(0, String::capacity)
                    + v.correlation_data.as_ref().map_or(0, Bytes::len)
                    + v.user_properties.capacity() * std::mem::size_of::<(String, String)>()
                    + v.user_properties
                        .iter()
                        .map(|(k, v)| k.capacity() + v.capacity())
                        .sum::<usize>()
                    + v.subscription_identifiers.capacity() * std::mem::size_of::<usize>()
            })
    }
    /// A caller may reject this bound before changing protocol state. Estimates
    /// include hash-table growth, not just the bytes represented on the wire.
    pub fn outgoing_memory_bound(&self, request: &Request) -> usize {
        let extra = match request {
            Request::Publish(p) => {
                Self::publication_memory_bound(p)
                    + p.properties
                        .as_ref()
                        .and_then(|v| v.topic_alias)
                        .filter(|_| !p.topic.is_empty())
                        .map_or(0, |id| {
                            p.topic.len() + Self::map_growth(&self.outgoing_aliases, &id)
                        })
            }
            Request::Subscribe(p) => {
                p.filters.len() * std::mem::size_of::<QoS>()
                    + Self::map_insert_growth(&self.outgoing_sub)
            }
            Request::Unsubscribe(_) => Self::map_insert_growth(&self.outgoing_unsub),
            Request::PubAck(p) => Self::map_growth(&self.incoming_done, &(p.pkid as usize)),
            Request::PubRec(p) if u8::from(p.reason) >= 128 => {
                Self::map_growth(&self.incoming_done, &(p.pkid as usize))
            }
            _ => 0,
        };
        self.retained_bytes()
            .saturating_add(extra)
            .saturating_add(1024)
    }
    pub fn incoming_memory_bound(&self, packet: &Packet) -> usize {
        let extra = match packet {
            Packet::Publish(p) => {
                let alias = p
                    .properties
                    .as_ref()
                    .and_then(|v| v.topic_alias)
                    .filter(|_| !p.topic.is_empty())
                    .map_or(0, |id| {
                        p.topic.len() + Self::map_growth(&self.topic_alises, &id)
                    });
                alias + Self::map_growth(&self.incoming_done, &(p.pkid as usize))
            }
            Packet::PubRel(p) => Self::map_growth(&self.incoming_done, &(p.pkid as usize)),
            _ => 0,
        };
        self.retained_bytes()
            .saturating_add(extra)
            .saturating_add(1024)
    }
    fn map_growth<K: Eq + std::hash::Hash, V>(map: &HashMap<K, V>, key: &K) -> usize {
        if map.contains_key(key) {
            0
        } else {
            Self::map_insert_growth(map)
        }
    }
    fn map_insert_growth<K, V>(map: &HashMap<K, V>) -> usize {
        if map.len() < map.capacity() {
            0
        } else {
            map.capacity().saturating_add(4).saturating_mul(256)
        }
    }
    pub fn configure_receive(
        &mut self,
        maximum: u16,
        aliases: u16,
        alias_bytes: usize,
        defer_write_completion: bool,
    ) -> Result<(), StateError> {
        if maximum == 0 || alias_bytes == 0 || self.incoming_inflight() != 0 {
            return Err(StateError::InvalidState);
        }
        self.receive_maximum = maximum as usize;
        self.receive_alias_maximum = aliases;
        self.alias_bytes_maximum = alias_bytes;
        self.defer_write_completion = defer_write_completion;
        Ok(())
    }
    pub fn next_available_packet_id(&mut self) -> Result<u16, StateError> {
        for _ in 0..self.max_outgoing_inflight_upper_limit {
            self.last_pkid = if self.last_pkid >= self.max_outgoing_inflight_upper_limit {
                1
            } else {
                self.last_pkid + 1
            };
            let id = self.last_pkid;
            if self.outgoing_pub[id as usize].is_none()
                && !self.outgoing_rel.contains(id as usize)
                && !self.outgoing_sub.contains_key(&id)
                && !self.outgoing_unsub.contains_key(&id)
            {
                if id == self.max_outgoing_inflight_upper_limit {
                    self.last_pkid = 0;
                }
                return Ok(id);
            }
        }
        Err(StateError::OutgoingCapacity)
    }
    pub fn acknowledgement_kind(&self, id: u16) -> Result<u8, StateError> {
        let id = id as usize;
        if !self.manual_acks || self.incoming_done.contains_key(&id) {
            return Err(StateError::InvalidState);
        }
        if self.incoming_ack.contains(id) {
            Ok(4)
        } else if self.incoming_rec.contains(id) {
            Ok(5)
        } else {
            Err(StateError::Unsolicited(id as u16))
        }
    }
    fn mark_completing(&mut self, id: usize) -> Result<u64, StateError> {
        if let Some(token) = self.incoming_done.get(&id) {
            return Ok(*token);
        }
        self.next_completion = self
            .next_completion
            .checked_add(65536)
            .ok_or(StateError::InvalidState)?;
        let token = self.next_completion | id as u64;
        self.incoming_done.insert(id, token);
        Ok(token)
    }
    /// Physical write tokens remain distinct from reusable MQTT packet identifiers.
    pub fn completion_token(&self, packet: &Packet) -> Option<u64> {
        let id = match packet {
            Packet::PubAck(p) => p.pkid,
            Packet::PubRec(p) => p.pkid,
            Packet::PubComp(p) => p.pkid,
            _ => return None,
        };
        self.incoming_done.get(&(id as usize)).copied()
    }
    pub fn acknowledgement_written(&mut self, token: u64) {
        let id = (token & 65535) as usize;
        if self.incoming_done.get(&id) == Some(&token) {
            self.incoming_done.remove(&id);
            self.incoming_ack.set(id, false);
            self.incoming_pub.set(id, false);
            self.incoming_rec.set(id, false);
        }
    }
    fn insert_alias(
        map: &mut HashMap<u16, Bytes>,
        alias: u16,
        topic: Bytes,
        maximum: usize,
    ) -> Result<(), StateError> {
        Self::check_alias(map, alias, &topic, maximum)?;
        map.insert(alias, topic);
        Ok(())
    }
    fn check_alias(
        map: &HashMap<u16, Bytes>,
        alias: u16,
        topic: &Bytes,
        maximum: usize,
    ) -> Result<(), StateError> {
        let bytes = map
            .iter()
            .filter(|(id, _)| **id != alias)
            .map(|(_, value)| value.len())
            .sum::<usize>();
        if bytes.saturating_add(topic.len()) > maximum {
            return Err(StateError::InvalidState);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::mqttbytes::v5::*;
    use super::mqttbytes::*;
    use super::{Event, Incoming, Outgoing, Request};
    use super::{MqttState, StateError};

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(u16::MAX, false)
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mut mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.next_pkid();

            // loops between 0-99. % 100 == 0 implies border
            let expected = i % 100;
            if expected == 0 {
                break;
            }

            assert_eq!(expected, pkid);
        }
    }

    #[test]
    fn outgoing_publish_should_set_pkid_and_add_publish_to_queue() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 3);
        assert_eq!(mqtt.inflight, 3);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 4);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn outgoing_publish_with_max_inflight_is_ok() {
        let mut mqtt = MqttState::new(2, false);

        // QoS2 publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be set back down to 0, since we hit the limit
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 2);

        // Capacity is an explicit refusal; the state must not retain/replay it.
        assert!(matches!(
            mqtt.outgoing_publish(publish.clone()),
            Err(StateError::OutgoingCapacity)
        ));
        assert_eq!(mqtt.inflight, 2);
        assert!(mqtt.collision.is_none());
        for id in [1, 2] {
            mqtt.handle_incoming_pubrec(&PubRec::new(id, None)).unwrap();
            mqtt.handle_incoming_pubcomp(&PubComp::new(id, None))
                .unwrap();
        }
        assert_eq!(mqtt.inflight, 0);
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.inflight, 1);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        if let Event::Outgoing(Outgoing::PubAck(pkid)) = mqtt.events[0] {
            assert_eq!(pkid, 2);
        } else {
            panic!("missing puback");
        }

        if let Event::Outgoing(Outgoing::PubRec(pkid)) = mqtt.events[1] {
            assert_eq!(pkid, 3);
        } else {
            panic!("missing PubRec");
        }
    }

    #[test]
    fn incoming_publish_should_not_be_acked_with_manual_acks() {
        let mut mqtt = build_mqttstate();
        mqtt.manual_acks = true;

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        assert!(mqtt.incoming_pub.contains(3));
        assert!(mqtt.events.is_empty());
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::AtLeastOnce);

        mqtt.outgoing_publish(publish1).unwrap();
        mqtt.outgoing_publish(publish2).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub[1].is_none());
        assert!(mqtt.outgoing_pub[2].is_none());
    }

    #[test]
    fn incoming_puback_with_pkid_greater_than_max_inflight_should_be_handled_gracefully() {
        let mut mqtt = build_mqttstate();

        let got = mqtt
            .handle_incoming_puback(&PubAck::new(101, None))
            .unwrap_err();

        match got {
            StateError::Unsolicited(pkid) => assert_eq!(pkid, 101),
            e => panic!("Unexpected error: {}", e),
        }
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.outgoing_publish(publish1);
        let _publish_out = mqtt.outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(&PubRec::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub[1].clone();
        assert_eq!(backup.unwrap().pkid, 1);

        // check if the qos2 element's release pkid is 2
        assert!(mqtt.outgoing_rel.contains(2));
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        match mqtt.outgoing_publish(publish).unwrap().unwrap() {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrec(&PubRec::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrel(&PubRel::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1, None)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1, None))
            .unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1, None)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp(PingResp))
            .unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }
}
