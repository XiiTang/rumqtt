use rumqttc::v5::{
    mqttbytes::{v5::*, QoS},
    Event, MqttState, Request,
};

fn publish(qos: QoS, id: u16, dup: bool) -> Publish {
    let mut p = Publish::new("fixture", qos, b"payload".to_vec(), None);
    p.pkid = id;
    p.dup = dup;
    p
}
fn state(manual: bool) -> MqttState {
    let mut state = MqttState::new(4, manual);
    state.configure_receive(1, 4, 100, true).unwrap();
    state
}

#[test]
fn wrong_qos_and_negative_outcomes_do_not_lose_or_replay_publications() {
    let mut s = state(false);
    s.handle_outgoing_packet(Request::Publish(publish(QoS::ExactlyOnce, 0, false)))
        .unwrap();
    assert!(s
        .handle_incoming_packet(Packet::PubAck(PubAck::new(1, None)))
        .is_err());
    assert_eq!(s.inflight(), 1);
    let mut rec = PubRec::new(1, None);
    rec.reason = 128u8.try_into().unwrap();
    assert!(s
        .handle_incoming_packet(Packet::PubRec(rec))
        .unwrap()
        .is_none());
    assert!(!s.pending());
    s.handle_outgoing_packet(Request::Publish(publish(QoS::ExactlyOnce, 0, false)))
        .unwrap();
    for _ in 0..2 {
        assert!(
            matches!(s.handle_incoming_packet(Packet::PubRec(PubRec::new(2, None))).unwrap(), Some(Packet::PubRel(p)) if p.pkid == 2)
        );
    }
    let mut comp = PubComp::new(2, None);
    comp.reason = PubCompReason::PacketIdentifierNotFound;
    assert!(s
        .handle_incoming_packet(Packet::PubComp(comp))
        .unwrap()
        .is_none());
    assert!(!s.pending());
}

#[test]
fn manual_qos2_duplicates_and_physical_completion_preserve_receive_credit() {
    let mut s = state(true);
    s.handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, false)))
        .unwrap();
    s.events.clear();
    assert!(s
        .handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, true)))
        .unwrap()
        .is_none());
    assert!(s.events.is_empty());
    assert!(s
        .handle_incoming_packet(Packet::PubRel(PubRel::new(42, None)))
        .is_err());
    assert_eq!(s.acknowledgement_kind(42).unwrap(), 5);
    s.handle_outgoing_packet(Request::PubRec(PubRec::new(42, None)))
        .unwrap();
    assert!(s.acknowledgement_kind(42).is_err());
    s.events.clear();
    assert!(matches!(
        s.handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, true)))
            .unwrap(),
        Some(Packet::PubRec(_))
    ));
    assert!(!s.events.iter().any(|e| matches!(e, Event::Incoming(_))));
    let reply = s
        .handle_incoming_packet(Packet::PubRel(PubRel::new(42, None)))
        .unwrap()
        .unwrap();
    let token = s.completion_token(&reply).unwrap();
    let duplicate = s
        .handle_incoming_packet(Packet::PubRel(PubRel::new(42, None)))
        .unwrap()
        .unwrap();
    assert_eq!(s.completion_token(&duplicate), Some(token));
    assert_eq!(s.incoming_inflight(), 1);
    assert!(s
        .handle_incoming_packet(Packet::Publish(publish(QoS::AtLeastOnce, 43, false)))
        .is_err());
    s.acknowledgement_written(token);
    assert_eq!(s.incoming_inflight(), 0);
    s.handle_incoming_packet(Packet::Publish(publish(QoS::ExactlyOnce, 42, false)))
        .unwrap();
    s.handle_outgoing_packet(Request::PubRec(PubRec::new(42, None)))
        .unwrap();
    let reply = s
        .handle_incoming_packet(Packet::PubRel(PubRel::new(42, None)))
        .unwrap()
        .unwrap();
    let next = s.completion_token(&reply).unwrap();
    assert_ne!(token, next);
    s.acknowledgement_written(token);
    assert_eq!(s.incoming_inflight(), 1);
    s.acknowledgement_written(next);
    assert_eq!(s.incoming_inflight(), 0);
}

#[test]
fn manual_negative_pubrec_and_qos1_ack_finish_only_after_write() {
    for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let mut s = state(true);
        s.handle_incoming_packet(Packet::Publish(publish(qos, 1, false)))
            .unwrap();
        let request = if qos == QoS::AtLeastOnce {
            Request::PubAck(PubAck::new(1, None))
        } else {
            let mut p = PubRec::new(1, None);
            p.reason = 128u8.try_into().unwrap();
            Request::PubRec(p)
        };
        let reply = s.handle_outgoing_packet(request).unwrap().unwrap();
        assert!(s.acknowledgement_kind(1).is_err());
        assert_eq!(s.incoming_inflight(), 1);
        s.acknowledgement_written(s.completion_token(&reply).unwrap());
        assert_eq!(s.incoming_inflight(), 0);
    }
}

#[test]
fn request_types_share_identifiers_and_suback_validates_before_releasing() {
    let mut s = MqttState::new(3, false);
    s.handle_outgoing_packet(Request::Publish(publish(QoS::ExactlyOnce, 0, false)))
        .unwrap();
    s.handle_incoming_packet(Packet::PubRec(PubRec::new(1, None)))
        .unwrap();
    let sub = Subscribe::new(Filter::new("fixture", QoS::AtMostOnce), None);
    assert!(
        matches!(s.handle_outgoing_packet(Request::Subscribe(sub.clone())).unwrap(), Some(Packet::Subscribe(p)) if p.pkid == 2)
    );
    assert!(
        matches!(s.handle_outgoing_packet(Request::Unsubscribe(Unsubscribe::new("fixture", None))).unwrap(), Some(Packet::Unsubscribe(p)) if p.pkid == 3)
    );
    assert!(s.handle_outgoing_packet(Request::Subscribe(sub)).is_err());
    assert!(s
        .handle_outgoing_packet(Request::Publish(publish(QoS::AtLeastOnce, 2, false)))
        .is_err());
    for codes in [vec![], vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)]] {
        assert!(s
            .handle_incoming_packet(Packet::SubAck(SubAck {
                pkid: 2,
                return_codes: codes,
                properties: None
            }))
            .is_err());
    }
    s.handle_incoming_packet(Packet::SubAck(SubAck {
        pkid: 2,
        return_codes: vec![128u8.try_into().unwrap()],
        properties: None,
    }))
    .unwrap();
    assert_eq!(s.next_available_packet_id().unwrap(), 2);
    assert!(s
        .handle_incoming_packet(Packet::UnsubAck(UnsubAck {
            pkid: 3,
            reasons: vec![],
            properties: None
        }))
        .is_err());
    s.handle_incoming_packet(Packet::UnsubAck(UnsubAck {
        pkid: 3,
        reasons: vec![UnsubAckReason::Success],
        properties: None,
    }))
    .unwrap();
    s.handle_incoming_packet(Packet::PubComp(PubComp::new(1, None)))
        .unwrap();
    assert!(!s.pending());
}

#[test]
fn aliases_are_resolved_and_rejected_before_outgoing_admission() {
    let mut s = state(false);
    let mut p = publish(QoS::AtMostOnce, 0, false);
    p.properties = Some(PublishProperties {
        topic_alias: Some(2),
        ..Default::default()
    });
    s.handle_incoming_packet(Packet::Publish(p.clone()))
        .unwrap();
    s.events.clear();
    p.topic = bytes::Bytes::new();
    s.handle_incoming_packet(Packet::Publish(p.clone()))
        .unwrap();
    assert!(
        matches!(s.events.pop_front(), Some(Event::Incoming(Packet::Publish(p))) if p.topic.as_ref() == b"fixture")
    );
    assert!(s.handle_outgoing_packet(Request::Publish(p)).is_err());
    s.handle_incoming_packet(Packet::ConnAck(ConnAck {
        session_present: false,
        code: ConnectReturnCode::Success,
        properties: Some(ConnAckProperties {
            topic_alias_max: Some(2),
            ..Default::default()
        }),
    }))
    .unwrap();
    let mut p = publish(QoS::AtLeastOnce, 0, false);
    p.topic = bytes::Bytes::from(vec![b'a'; 101]);
    p.properties = Some(PublishProperties {
        topic_alias: Some(2),
        ..Default::default()
    });
    assert!(s.handle_outgoing_packet(Request::Publish(p)).is_err());
    assert!(!s.pending());
    assert_eq!(s.next_available_packet_id().unwrap(), 1);
}

#[test]
fn initial_allocation_is_admissible_before_construction_and_ping_is_correlated() {
    let mut s = MqttState::new(1024, false);
    assert!(s.retained_bytes() <= MqttState::initial_memory_bound(1024));
    assert!(s.configure_receive(0, 0, 100, true).is_err());
    assert!(s
        .handle_incoming_packet(Packet::PingResp(PingResp))
        .is_err());
    s.handle_outgoing_packet(Request::PingReq).unwrap();
    s.handle_incoming_packet(Packet::PingResp(PingResp))
        .unwrap();
    assert!(s
        .handle_incoming_packet(Packet::PingResp(PingResp))
        .is_err());
}

#[test]
fn admission_bounds_include_table_growth_and_property_allocations() {
    let mut s = MqttState::new(512, false);
    s.configure_receive(512, 512, 4096, true).unwrap();
    for id in 1..=512 {
        let mut p = publish(QoS::AtLeastOnce, 0, false);
        p.properties = Some(PublishProperties {
            user_properties: vec![(String::new(), String::new()); 64],
            ..Default::default()
        });
        let request = Request::Publish(p);
        let bound = s.outgoing_memory_bound(&request);
        s.handle_outgoing_packet(request).unwrap();
        s.events.clear();
        assert!(s.retained_bytes() <= bound, "outgoing allocation {id}");
        let mut p = publish(QoS::AtLeastOnce, id, false);
        p.properties = Some(PublishProperties {
            topic_alias: Some(id),
            ..Default::default()
        });
        let packet = Packet::Publish(p);
        let bound = s.incoming_memory_bound(&packet);
        s.handle_incoming_packet(packet).unwrap();
        s.events.clear();
        assert!(s.retained_bytes() <= bound, "incoming allocation {id}");
    }
}

#[test]
fn resume_preserves_alias_target_after_reassignment_and_respects_new_window() {
    let mut s = state(false);
    s.handle_incoming_packet(Packet::ConnAck(ConnAck {
        session_present: false,
        code: ConnectReturnCode::Success,
        properties: Some(ConnAckProperties {
            topic_alias_max: Some(1),
            ..Default::default()
        }),
    }))
    .unwrap();
    for topic in ["first", "", "changed"] {
        let p = Publish::new(
            topic,
            QoS::AtLeastOnce,
            b"x".to_vec(),
            Some(PublishProperties {
                topic_alias: Some(1),
                ..Default::default()
            }),
        );
        s.handle_outgoing_packet(Request::Publish(p)).unwrap();
    }
    s.resume_session();
    s.handle_incoming_packet(Packet::ConnAck(ConnAck {
        session_present: true,
        code: ConnectReturnCode::Success,
        properties: Some(ConnAckProperties {
            receive_max: Some(1),
            ..Default::default()
        }),
    }))
    .unwrap();
    for expected in ["first", "first", "changed"] {
        let Packet::Publish(p) = s.next_resumed_packet().unwrap() else {
            panic!()
        };
        assert_eq!(p.topic.as_ref(), expected.as_bytes());
        assert!(p.dup);
        assert!(p.properties.unwrap().topic_alias.is_none());
        assert!(!s.has_resumed_packet());
        s.handle_incoming_packet(Packet::PubAck(PubAck::new(p.pkid, None)))
            .unwrap();
    }
    assert!(!s.pending());
}

#[test]
fn expired_publication_is_not_replayed_after_resume() {
    let mut s = state(false);
    s.handle_outgoing_packet(Request::Publish(Publish::new(
        "expired",
        QoS::AtLeastOnce,
        b"x".to_vec(),
        Some(PublishProperties {
            message_expiry_interval: Some(0),
            ..Default::default()
        }),
    )))
    .unwrap();
    s.resume_session();
    assert!(s.next_resumed_packet().is_none());
    assert!(!s.pending());
}

#[test]
fn cancelled_before_writer_publication_is_not_transmitted_by_resume() {
    let mut s = state(false);
    let Packet::Publish(p) = s
        .handle_outgoing_packet(Request::Publish(publish(QoS::AtLeastOnce, 0, false)))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    s.track_publish_transmission(
        p.pkid,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    s.resume_session();
    assert!(!s.has_resumed_packet());
    assert!(!s.pending());
}
