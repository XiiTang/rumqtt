use crate::{Event, Incoming, Outgoing, Request};
use std::sync::{
    atomic::{AtomicBool as RuntimeAtomicBool, Ordering as RuntimeOrdering},
    Arc as RuntimeArc,
};

use crate::mqttbytes::v4::*;
use crate::mqttbytes::{self, *};
use fixedbitset::FixedBitSet;
use std::collections::{BTreeMap, VecDeque};
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
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
    Deserialization(#[from] mqttbytes::Error),
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
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
    /// Packet id of the last acked publish
    pub(crate) last_puback: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Maximum number of allowed inflight
    pub(crate) max_inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: FixedBitSet,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: FixedBitSet,
    /// QoS 1 publications awaiting an application acknowledgement.
    incoming_ack: FixedBitSet,
    /// QoS 2 publications awaiting an application PUBREC.
    incoming_rec: FixedBitSet,
    /// Subscription acknowledgement correlation, sharing the packet-id namespace.
    outgoing_sub: BTreeMap<u16, Vec<QoS>>,
    outgoing_unsub: FixedBitSet,
    /// Last collision due to broker not acking in order
    pub collision: Option<Publish>,
    /// Buffered incoming packets
    pub events: VecDeque<Event>,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
}

impl MqttState {
    /// Conservative initial allocation bound for pre-admission before creating
    /// the state or connecting a transport. The fixed-bitset allowances include
    /// 64 bytes of block/alignment rounding per allocation. Regression tests
    /// compare this bound with the actual retained allocations at boundary sizes.
    pub fn initial_memory_bound(max_inflight: u16) -> usize {
        let slots = usize::from(max_inflight) + 1;
        std::mem::size_of::<Self>()
            + slots * std::mem::size_of::<Option<Publish>>()
            + 3 * (65536 / 8 + 64)
            + 2 * (slots.div_ceil(8) + 64)
            + 100 * std::mem::size_of::<Event>()
    }

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
            last_puback: 0,
            inflight: 0,
            max_inflight,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: FixedBitSet::with_capacity(max_inflight as usize + 1),
            incoming_pub: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            incoming_ack: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            incoming_rec: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            outgoing_sub: BTreeMap::new(),
            outgoing_unsub: FixedBitSet::with_capacity(max_inflight as usize + 1),
            collision: None,
            // TODO: Optimize these sizes later
            events: VecDeque::with_capacity(100),
            manual_acks,
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Request> {
        self.transmission_flags.clear();
        let mut pending = Vec::with_capacity(100);
        let (first_half, second_half) = self
            .outgoing_pub
            .split_at_mut(self.last_puback as usize + 1);

        for publish in second_half.iter_mut().chain(first_half) {
            if let Some(publish) = publish.take() {
                let request = Request::Publish(publish);
                pending.push(request);
            }
        }

        // remove and collect pending releases
        for pkid in self.outgoing_rel.ones() {
            let request = Request::PubRel(PubRel::new(pkid as u16));
            pending.push(request);
        }
        self.outgoing_rel.clear();

        // remove packet ids of incoming qos2 publishes
        self.incoming_pub.clear();
        self.incoming_ack.clear();
        self.incoming_rec.clear();
        self.outgoing_sub.clear();
        self.outgoing_unsub.clear();

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.inflight = 0;
        pending
    }

    /// Resume the same broker session without losing QoS packet identities.
    /// The caller must verify CONNACK Session Present before invoking this.
    /// These are protocol retransmissions, not new application publications.
    /// SUBSCRIBE/UNSUBSCRIBE results interrupted by the old transport are unknown;
    /// their identifiers are returned and released without replaying operations.
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
    pub fn resume_session(&mut self) -> (Vec<Packet>, Vec<u16>) {
        self.discard_unsent_publications();
        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.last_incoming = Instant::now();
        self.last_outgoing = Instant::now();
        self.events.clear();
        let mut interrupted: Vec<_> = self.outgoing_sub.keys().copied().collect();
        interrupted.extend(self.outgoing_unsub.ones().map(|id| id as u16));
        self.outgoing_sub.clear();
        self.outgoing_unsub.clear();
        let mut packets = Vec::new();
        let (first, second) = self.outgoing_pub.split_at(self.last_puback as usize + 1);
        for publish in second.iter().chain(first).flatten() {
            let mut publish = publish.clone();
            publish.dup = true;
            packets.push(Packet::Publish(publish));
        }
        for id in self.outgoing_rel.ones() {
            packets.push(Packet::PubRel(PubRel::new(id as u16)));
        }
        (packets, interrupted)
    }

    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Whether protocol acknowledgements for outgoing requests remain outstanding.
    pub fn pending(&self) -> bool {
        self.inflight != 0 || !self.outgoing_sub.is_empty() || !self.outgoing_unsub.is_clear()
    }

    /// Incoming QoS exchanges retained by this state machine.
    pub fn incoming_inflight(&self) -> usize {
        self.incoming_ack.count_ones(..) + self.incoming_pub.count_ones(..)
    }

    /// Conservative retained allocation estimate; callers should drain `events`
    /// after every transition before querying this value. Shared publish bytes
    /// are counted in full. No allocator-specific tree-node layout is assumed.
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.transmission_flags.len() * 128
            + self.outgoing_pub.capacity() * std::mem::size_of::<Option<Publish>>()
            + self
                .outgoing_pub
                .iter()
                .flatten()
                .map(|p| p.topic.capacity() + p.payload.len())
                .sum::<usize>()
            + [
                &self.outgoing_rel,
                &self.incoming_pub,
                &self.incoming_ack,
                &self.incoming_rec,
                &self.outgoing_unsub,
            ]
            .iter()
            .map(|set| std::mem::size_of_val(set.as_slice()))
            .sum::<usize>()
            + self
                .outgoing_sub
                .values()
                .map(|qos| 128 + qos.capacity() * std::mem::size_of::<QoS>())
                .sum::<usize>()
            + self.events.capacity() * std::mem::size_of::<Event>()
    }

    /// Find a free identifier without creating a collision/replay request. The
    /// identifier is claimed by the immediately following outgoing transition.
    pub fn next_available_packet_id(&mut self) -> Result<u16, StateError> {
        for _ in 0..self.max_inflight {
            let id = self.next_pkid();
            if self.outgoing_pub[id as usize].is_none()
                && !self.outgoing_rel.contains(id as usize)
                && !self.outgoing_sub.contains_key(&id)
                && !self.outgoing_unsub.contains(id as usize)
            {
                return Ok(id);
            }
        }
        Err(StateError::InvalidState)
    }

    /// Build the correct manual acknowledgement from the library-owned state.
    /// Passing the returned request through `handle_outgoing_packet` commits it.
    pub fn acknowledgement(&self, id: u16) -> Result<Request, StateError> {
        if !self.manual_acks {
            return Err(StateError::InvalidState);
        }
        if self.incoming_ack.contains(id as usize) {
            Ok(Request::PubAck(PubAck::new(id)))
        } else if self.incoming_rec.contains(id as usize) {
            Ok(Request::PubRec(PubRec::new(id)))
        } else {
            Err(StateError::Unsolicited(id))
        }
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
            Request::PingReq(_) => self.outgoing_ping()?,
            Request::Disconnect(_) => self.outgoing_disconnect()?,
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
        packet: Incoming,
    ) -> Result<Option<Packet>, StateError> {
        let duplicate = matches!(&packet, Incoming::Publish(p)
            if p.qos == QoS::ExactlyOnce && self.incoming_pub.contains(p.pkid as usize));
        // Only successful first delivery enters the user event stream. Controls
        // for duplicates still progress and are returned to the transport.
        let event_index = self.events.len();

        let outgoing = match &packet {
            Incoming::PingResp => self.handle_incoming_pingresp()?,
            Incoming::Publish(publish) => self.handle_incoming_publish(publish)?,
            Incoming::SubAck(suback) => self.handle_incoming_suback(suback)?,
            Incoming::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback)?,
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback)?,
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec)?,
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel)?,
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp)?,
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };
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

    fn handle_incoming_suback(&mut self, ack: &SubAck) -> Result<Option<Packet>, StateError> {
        let requested = self
            .outgoing_sub
            .get(&ack.pkid)
            .ok_or(StateError::Unsolicited(ack.pkid))?;
        if requested.len() != ack.return_codes.len() || requested.iter().zip(&ack.return_codes)
            .any(|(requested, actual)| matches!(actual, SubscribeReasonCode::Success(qos) if *qos as u8 > *requested as u8)) {
            return Err(StateError::WrongPacket);
        }
        self.outgoing_sub.remove(&ack.pkid);
        Ok(None)
    }

    fn handle_incoming_unsuback(&mut self, ack: &UnsubAck) -> Result<Option<Packet>, StateError> {
        if !self.outgoing_unsub.contains(ack.pkid as usize) {
            return Err(StateError::Unsolicited(ack.pkid));
        }
        self.outgoing_unsub.set(ack.pkid as usize, false);
        Ok(None)
    }

    fn handle_incoming_publish(&mut self, publish: &Publish) -> Result<Option<Packet>, StateError> {
        let id = publish.pkid as usize;
        if publish.qos != QoS::AtMostOnce && id == 0 {
            return Err(StateError::WrongPacket);
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
                if !self.manual_acks {
                    return self.outgoing_puback(PubAck::new(publish.pkid));
                }
                Ok(None)
            }
            QoS::ExactlyOnce => {
                if self.incoming_ack.contains(id) {
                    return Err(StateError::WrongPacket);
                }
                if self.incoming_pub.contains(id) {
                    if !publish.dup {
                        return Err(StateError::WrongPacket);
                    }
                    if self.incoming_rec.contains(id) {
                        return Ok(None);
                    }
                    return self.outgoing_pubrec(PubRec::new(publish.pkid));
                }
                self.incoming_pub.insert(id);
                self.incoming_rec.insert(id);
                if !self.manual_acks {
                    return self.outgoing_pubrec(PubRec::new(publish.pkid));
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

        if !matches!(publish, Some(p) if p.qos == QoS::AtLeastOnce) {
            return Err(StateError::Unsolicited(puback.pkid));
        }
        self.last_puback = puback.pkid;

        if publish.take().is_none() {
            error!("Unsolicited puback packet: {:?}", puback.pkid);
            return Err(StateError::Unsolicited(puback.pkid));
        }

        self.inflight -= 1;
        let packet = self.check_collision(puback.pkid).map(|publish| {
            self.outgoing_pub[publish.pkid as usize] = Some(publish.clone());
            self.inflight += 1;

            let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            Packet::Publish(publish)
        });

        Ok(packet)
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
        if self.outgoing_rel.contains(pubrec.pkid as usize) {
            self.events
                .push_back(Event::Outgoing(Outgoing::PubRel(pubrec.pkid)));
            return Ok(Some(Packet::PubRel(PubRel::new(pubrec.pkid))));
        }
        let publish = self
            .outgoing_pub
            .get_mut(pubrec.pkid as usize)
            .ok_or(StateError::Unsolicited(pubrec.pkid))?;

        if !matches!(publish, Some(p) if p.qos == QoS::ExactlyOnce) {
            return Err(StateError::Unsolicited(pubrec.pkid));
        }
        if publish.take().is_none() {
            error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
            return Err(StateError::Unsolicited(pubrec.pkid));
        }

        // NOTE: Inflight - 1 for qos2 in comp
        self.outgoing_rel.insert(pubrec.pkid as usize);
        let pubrel = PubRel { pkid: pubrec.pkid };
        let event = Event::Outgoing(Outgoing::PubRel(pubrec.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(pubrel)))
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
        if pubrel.pkid == 0
            || self.incoming_ack.contains(pubrel.pkid as usize)
            || self.incoming_rec.contains(pubrel.pkid as usize)
        {
            return Err(StateError::Unsolicited(pubrel.pkid));
        }
        // MQTT 3.1.1 requires PUBCOMP even for an already completed exchange.
        self.incoming_pub.set(pubrel.pkid as usize, false);
        let event = Event::Outgoing(Outgoing::PubComp(pubrel.pkid));
        let pubcomp = PubComp { pkid: pubrel.pkid };
        self.events.push_back(event);

        Ok(Some(Packet::PubComp(pubcomp)))
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
        if !self.outgoing_rel.contains(pubcomp.pkid as usize) {
            error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }

        self.outgoing_rel.set(pubcomp.pkid as usize, false);
        self.inflight -= 1;
        let packet = self.check_collision(pubcomp.pkid).map(|publish| {
            let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            Packet::Publish(publish)
        });

        Ok(packet)
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
        if publish.qos != QoS::AtMostOnce {
            if publish.pkid == 0 {
                publish.pkid = self.next_available_packet_id()?;
            }

            let pkid = publish.pkid;
            if self.outgoing_rel.contains(pkid as usize)
                || self.outgoing_sub.contains_key(&pkid)
                || self.outgoing_unsub.contains(pkid as usize)
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
            self.outgoing_pub[pkid as usize] = Some(publish.clone());
            self.inflight += 1;
        };

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Publish(publish)))
    }

    fn outgoing_pubrel(&mut self, pubrel: PubRel) -> Result<Option<Packet>, StateError> {
        let pubrel = self.save_pubrel(pubrel)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);
        let event = Event::Outgoing(Outgoing::PubRel(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(pubrel)))
    }

    fn outgoing_puback(&mut self, puback: PubAck) -> Result<Option<Packet>, StateError> {
        if !self.incoming_ack.contains(puback.pkid as usize) {
            return Err(StateError::Unsolicited(puback.pkid));
        }
        self.incoming_ack.set(puback.pkid as usize, false);
        let event = Event::Outgoing(Outgoing::PubAck(puback.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubAck(puback)))
    }

    fn outgoing_pubrec(&mut self, pubrec: PubRec) -> Result<Option<Packet>, StateError> {
        if !self.incoming_pub.contains(pubrec.pkid as usize) {
            return Err(StateError::Unsolicited(pubrec.pkid));
        }
        self.incoming_rec.set(pubrec.pkid as usize, false);
        let event = Event::Outgoing(Outgoing::PubRec(pubrec.pkid));
        self.events.push_back(event);

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
            "Pingreq,
            last incoming packet before {} millisecs,
            last outgoing request before {} millisecs",
            elapsed_in.as_millis(),
            elapsed_out.as_millis()
        );

        let event = Event::Outgoing(Outgoing::PingReq);
        self.events.push_back(event);

        Ok(Some(Packet::PingReq))
    }

    fn outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
    ) -> Result<Option<Packet>, StateError> {
        if subscription.filters.is_empty() {
            return Err(StateError::EmptySubscription);
        }

        let pkid = self.next_available_packet_id()?;
        subscription.pkid = pkid;
        self.outgoing_sub.insert(
            pkid,
            subscription
                .filters
                .iter()
                .map(|filter| filter.qos)
                .collect(),
        );

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.filters, subscription.pkid
        );

        let event = Event::Outgoing(Outgoing::Subscribe(subscription.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Subscribe(subscription)))
    }

    fn outgoing_unsubscribe(
        &mut self,
        mut unsub: Unsubscribe,
    ) -> Result<Option<Packet>, StateError> {
        if unsub.topics.is_empty() {
            return Err(StateError::EmptySubscription);
        }
        let pkid = self.next_available_packet_id()?;
        unsub.pkid = pkid;
        self.outgoing_unsub.insert(pkid as usize);

        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.topics, unsub.pkid
        );

        let event = Event::Outgoing(Outgoing::Unsubscribe(unsub.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Unsubscribe(unsub)))
    }

    fn outgoing_disconnect(&mut self) -> Result<Option<Packet>, StateError> {
        debug!("Disconnect");

        let event = Event::Outgoing(Outgoing::Disconnect);
        self.events.push_back(event);

        Ok(Some(Packet::Disconnect))
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
        if pubrel.pkid == 0 {
            pubrel.pkid = self.next_available_packet_id()?;
        }
        let id = pubrel.pkid as usize;
        if id >= self.outgoing_pub.len()
            || self.outgoing_pub[id].is_some()
            || self.outgoing_sub.contains_key(&pubrel.pkid)
            || self.outgoing_unsub.contains(id)
        {
            return Err(StateError::InvalidState);
        }
        if !self.outgoing_rel.contains(id) {
            self.outgoing_rel.insert(id);
            self.inflight += 1;
        }
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
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

#[cfg(test)]
mod test {
    use super::{MqttState, StateError};
    use crate::mqttbytes::v4::*;
    use crate::mqttbytes::*;
    use crate::{Event, Incoming, Outgoing, Request};

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(100, false)
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
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

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
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        assert!(mqtt.incoming_pub.contains(3));

        assert!(mqtt.events.is_empty());
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let packet = mqtt.handle_incoming_publish(&publish).unwrap().unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", packet),
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

        mqtt.handle_incoming_puback(&PubAck::new(1)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck::new(2)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub[1].is_none());
        assert!(mqtt.outgoing_pub[2].is_none());
    }

    #[test]
    fn incoming_puback_with_pkid_greater_than_max_inflight_should_be_handled_gracefully() {
        let mut mqtt = build_mqttstate();

        let got = mqtt.handle_incoming_puback(&PubAck::new(101)).unwrap_err();

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

        mqtt.handle_incoming_pubrec(&PubRec::new(2)).unwrap();
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
        let packet = mqtt.outgoing_publish(publish).unwrap().unwrap();
        match packet {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        let packet = mqtt
            .handle_incoming_pubrec(&PubRec::new(1))
            .unwrap()
            .unwrap();
        match packet {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let packet = mqtt.handle_incoming_publish(&publish).unwrap().unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        let packet = mqtt
            .handle_incoming_pubrel(&PubRel::new(1))
            .unwrap()
            .unwrap();
        match packet {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1)).unwrap();
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
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1)))
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
        mqtt.handle_incoming_packet(Incoming::PingResp).unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }

    #[test]
    fn clean_is_calculating_pending_correctly() {
        let mut mqtt = build_mqttstate();

        fn build_outgoing_pub() -> Vec<Option<Publish>> {
            vec![
                None,
                Some(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "test".to_string(),
                    pkid: 1,
                    payload: "".into(),
                }),
                Some(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "test".to_string(),
                    pkid: 2,
                    payload: "".into(),
                }),
                Some(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "test".to_string(),
                    pkid: 3,
                    payload: "".into(),
                }),
                None,
                None,
                Some(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "test".to_string(),
                    pkid: 6,
                    payload: "".into(),
                }),
            ]
        }

        mqtt.outgoing_pub = build_outgoing_pub();
        mqtt.last_puback = 3;
        let requests = mqtt.clean();
        let res = vec![6, 1, 2, 3];
        for (req, idx) in requests.iter().zip(res) {
            if let Request::Publish(publish) = req {
                assert_eq!(publish.pkid, idx);
            } else {
                unreachable!()
            }
        }

        mqtt.outgoing_pub = build_outgoing_pub();
        mqtt.last_puback = 0;
        let requests = mqtt.clean();
        let res = vec![1, 2, 3, 6];
        for (req, idx) in requests.iter().zip(res) {
            if let Request::Publish(publish) = req {
                assert_eq!(publish.pkid, idx);
            } else {
                unreachable!()
            }
        }

        mqtt.outgoing_pub = build_outgoing_pub();
        mqtt.last_puback = 6;
        let requests = mqtt.clean();
        let res = vec![1, 2, 3, 6];
        for (req, idx) in requests.iter().zip(res) {
            if let Request::Publish(publish) = req {
                assert_eq!(publish.pkid, idx);
            } else {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod runtime_resume_tests {
    use super::*;
    #[test]
    fn resume_preserves_publish_ids_and_incoming_qos2_deduplication() {
        let mut state = MqttState::new(10, false);
        let original = state
            .handle_outgoing_packet(Request::Publish(Publish::new(
                "out",
                QoS::AtLeastOnce,
                "body",
            )))
            .unwrap()
            .unwrap();
        let Packet::Publish(original) = original else {
            panic!()
        };
        let mut incoming = Publish::new("in", QoS::ExactlyOnce, "body");
        incoming.pkid = 7;
        state
            .handle_incoming_packet(Packet::Publish(incoming.clone()))
            .unwrap();
        let (packets, interrupted) = state.resume_session();
        assert!(interrupted.is_empty());
        let Packet::Publish(resumed) = &packets[0] else {
            panic!()
        };
        assert_eq!(resumed.pkid, original.pkid);
        assert!(resumed.dup);
        state
            .handle_incoming_packet(Packet::PubAck(PubAck::new(original.pkid)))
            .unwrap();
        assert!(!state.pending());
        state.events.clear();
        incoming.dup = true;
        state
            .handle_incoming_packet(Packet::Publish(incoming))
            .unwrap();
        assert!(!state
            .events
            .iter()
            .any(|e| matches!(e, Event::Incoming(Packet::Publish(_)))));
    }
}
