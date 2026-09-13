use rumqttc::{
    mqttbytes::{v4::*, QoS},
    Event, MqttState, Request,
};

fn publish(qos: QoS, id: u16, dup: bool) -> Publish {
    let mut p = Publish::new("fixture", qos, b"payload".to_vec());
    p.pkid = id;
    p.dup = dup;
    p
}

#[test]
fn manual_qos2_duplicates_preserve_application_ack_boundary() {
    let mut state = MqttState::new(4, true);
    assert_eq!(
        state
            .handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, false)))
            .unwrap(),
        None
    );
    assert!(matches!(
        state.events.pop_front(),
        Some(Event::Incoming(Packet::Publish(_)))
    ));
    assert_eq!(
        state
            .handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, true)))
            .unwrap(),
        None
    );
    assert!(state.events.is_empty());
    assert!(state
        .handle_incoming_packet(Packet::PubRel(PubRel::new(42)))
        .is_err());
    let ack = state.acknowledgement(42).unwrap();
    assert_eq!(
        state.handle_outgoing_packet(ack).unwrap(),
        Some(Packet::PubRec(PubRec::new(42)))
    );
    state.events.clear();
    assert_eq!(
        state
            .handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, true)))
            .unwrap(),
        Some(Packet::PubRec(PubRec::new(42)))
    );
    assert!(!state.events.iter().any(|e| matches!(e, Event::Incoming(_))));
    assert!(state.acknowledgement(42).is_err());
    for _ in 0..2 {
        assert_eq!(
            state
                .handle_incoming_packet(Packet::PubRel(PubRel::new(42)))
                .unwrap(),
            Some(Packet::PubComp(PubComp::new(42)))
        );
    }
    assert_eq!(state.incoming_inflight(), 0);
}

#[test]
fn manual_qos1_ack_is_correlated_and_cannot_be_repeated() {
    let mut state = MqttState::new(4, true);
    state
        .handle_incoming_packet(Packet::Publish(publish(QoS::AtLeastOnce, 42, false)))
        .unwrap();
    assert!(state
        .handle_outgoing_packet(Request::PubRec(PubRec::new(42)))
        .is_err());
    assert_eq!(state.incoming_inflight(), 1);
    let ack = state.acknowledgement(42).unwrap();
    assert_eq!(
        state.handle_outgoing_packet(ack).unwrap(),
        Some(Packet::PubAck(PubAck::new(42)))
    );
    assert!(state.acknowledgement(42).is_err());
    assert_eq!(state.incoming_inflight(), 0);
}

#[test]
fn subscribe_unsubscribe_and_qos2_release_share_identifier_space() {
    let mut state = MqttState::new(3, false);
    assert!(
        matches!(state.handle_outgoing_packet(Request::Publish(publish(QoS::ExactlyOnce, 0, false))).unwrap(), Some(Packet::Publish(p)) if p.pkid == 1)
    );
    state
        .handle_incoming_packet(Packet::PubRec(PubRec::new(1)))
        .unwrap();
    let sub = Subscribe {
        pkid: 0,
        filters: vec![SubscribeFilter::new("fixture".into(), QoS::AtMostOnce)],
    };
    assert!(
        matches!(state.handle_outgoing_packet(Request::Subscribe(sub.clone())).unwrap(), Some(Packet::Subscribe(p)) if p.pkid == 2)
    );
    assert!(
        matches!(state.handle_outgoing_packet(Request::Unsubscribe(Unsubscribe { pkid: 0, topics: vec!["fixture".into()] })).unwrap(), Some(Packet::Unsubscribe(p)) if p.pkid == 3)
    );
    assert!(state
        .handle_outgoing_packet(Request::Subscribe(sub))
        .is_err());
    assert!(state
        .handle_incoming_packet(Packet::SubAck(SubAck::new(
            1,
            vec![SubscribeReasonCode::Failure]
        )))
        .is_err());
    assert!(state
        .handle_incoming_packet(Packet::SubAck(SubAck::new(2, vec![])))
        .is_err());
    assert!(state
        .handle_incoming_packet(Packet::SubAck(SubAck::new(
            2,
            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)]
        )))
        .is_err());
    state
        .handle_incoming_packet(Packet::SubAck(SubAck::new(
            2,
            vec![SubscribeReasonCode::Failure],
        )))
        .unwrap();
    assert_eq!(state.next_available_packet_id().unwrap(), 2);
    assert!(state.pending());
    state
        .handle_incoming_packet(Packet::UnsubAck(UnsubAck::new(3)))
        .unwrap();
    state
        .handle_incoming_packet(Packet::PubComp(PubComp::new(1)))
        .unwrap();
    assert!(!state.pending());
}

#[test]
fn wrong_qos_ack_does_not_discard_pending_publish_and_pubrec_can_repeat() {
    let mut state = MqttState::new(2, false);
    state
        .handle_outgoing_packet(Request::Publish(publish(QoS::ExactlyOnce, 0, false)))
        .unwrap();
    assert!(state
        .handle_incoming_packet(Packet::PubAck(PubAck::new(1)))
        .is_err());
    assert_eq!(state.inflight(), 1);
    for _ in 0..2 {
        assert_eq!(
            state
                .handle_incoming_packet(Packet::PubRec(PubRec::new(1)))
                .unwrap(),
            Some(Packet::PubRel(PubRel::new(1)))
        );
    }
    state
        .handle_incoming_packet(Packet::PubComp(PubComp::new(1)))
        .unwrap();
    assert_eq!(state.inflight(), 0);
}

#[test]
fn unsupported_request_is_an_error_and_unsolicited_ping_is_rejected() {
    let mut state = MqttState::new(2, false);
    assert!(state
        .handle_outgoing_packet(Request::SubAck(SubAck::new(1, vec![])))
        .is_err());
    assert!(state.handle_incoming_packet(Packet::PingResp).is_err());
    assert!(state.events.is_empty());
    assert_eq!(
        state
            .handle_outgoing_packet(Request::PingReq(PingReq))
            .unwrap(),
        Some(Packet::PingReq)
    );
    state.handle_incoming_packet(Packet::PingResp).unwrap();
    assert!(!state.await_pingresp);
}

#[test]
fn explicit_release_is_bounded_and_repeating_it_does_not_inflate_state() {
    let mut state = MqttState::new(2, false);
    assert!(state
        .handle_outgoing_packet(Request::PubRel(PubRel::new(65535)))
        .is_err());
    for _ in 0..2 {
        state
            .handle_outgoing_packet(Request::PubRel(PubRel::new(2)))
            .unwrap();
        assert_eq!(state.inflight(), 1);
    }
    state
        .handle_incoming_packet(Packet::PubComp(PubComp::new(2)))
        .unwrap();
    assert!(!state.pending());
}
