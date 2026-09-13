use bytes::{Bytes, BytesMut};
use rumqttc::v5::mqttbytes::{v5::*, QoS};

#[test]
fn ordered_publish_uses_typed_values_once_and_preserves_repeated_user_order() {
    let packet = Packet::Publish(Publish::new(
        "t",
        QoS::AtMostOnce,
        Bytes::from_static(&[255]),
        Some(PublishProperties {
            content_type: Some("x".into()),
            user_properties: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            ..Default::default()
        }),
    ));
    let order = PropertyOrder {
        packet: vec![38, 3, 38],
        will: vec![],
    };
    let mut bytes = BytesMut::new();
    let n = packet
        .write_with_property_order(&mut bytes, Some(100), &order)
        .unwrap();
    let expected = b"\x30\x17\0\x01t\x12\x26\0\x01a\0\x011\x03\0\x01x\x26\0\x01b\0\x012\xff";
    assert_eq!(bytes.as_ref(), expected);
    assert_eq!(n, expected.len());
    assert_eq!(
        Packet::read_with_property_order(&mut bytes, Some(100)).unwrap(),
        DecodedPacket {
            packet,
            property_order: order
        }
    );
}

#[test]
fn invalid_inventory_and_limit_leave_existing_output_untouched() {
    let packet = Packet::Publish(Publish::new(
        "t",
        QoS::AtMostOnce,
        Bytes::new(),
        Some(PublishProperties {
            content_type: Some("x".into()),
            ..Default::default()
        }),
    ));
    for order in [vec![], vec![38], vec![3, 3]] {
        let mut bytes = BytesMut::from(&b"prefix"[..]);
        assert!(packet
            .write_with_property_order(
                &mut bytes,
                Some(100),
                &PropertyOrder {
                    packet: order,
                    will: vec![]
                }
            )
            .is_err());
        assert_eq!(bytes.as_ref(), b"prefix");
    }
    let mut bytes = BytesMut::new();
    assert!(packet
        .write_with_property_order(
            &mut bytes,
            Some(1),
            &PropertyOrder {
                packet: vec![3],
                will: vec![]
            }
        )
        .is_err());
    assert!(bytes.is_empty());
}

#[test]
fn connect_preserves_empty_username_binary_password_and_append_offset() {
    let packet = Packet::Connect(
        Connect {
            keep_alive: 0,
            client_id: "c".into(),
            clean_start: true,
            properties: None,
        },
        None,
        Some(Login {
            username: Some(String::new()),
            password: Some(Bytes::from_static(&[0, 255])),
        }),
    );
    let mut bytes = BytesMut::from(&b"prefix"[..]);
    let n = packet
        .write_with_property_order(&mut bytes, Some(100), &PropertyOrder::default())
        .unwrap();
    assert_eq!(&bytes[..6], b"prefix");
    let mut frame = bytes.split_off(6);
    assert_eq!(n, frame.len());
    assert_eq!(frame[9], 0xc2);
    assert_eq!(Packet::read(&mut frame, Some(100)).unwrap(), packet);
}

#[test]
fn disconnect_lengths_and_property_order_cover_empty_and_negative_outcomes() {
    for reason in [
        DisconnectReasonCode::NormalDisconnection,
        DisconnectReasonCode::DisconnectWithWillMessage,
        DisconnectReasonCode::UnspecifiedError,
    ] {
        for properties in [
            None,
            Some(DisconnectProperties {
                session_expiry_interval: None,
                reason_string: None,
                user_properties: vec![],
                server_reference: None,
            }),
        ] {
            let packet = Packet::Disconnect(Disconnect {
                reason_code: reason,
                properties,
            });
            let mut bytes = BytesMut::new();
            let n = packet
                .write_with_property_order(&mut bytes, Some(100), &PropertyOrder::default())
                .unwrap();
            assert_eq!(n, bytes.len());
            assert_eq!(n, packet.size());
            let decoded = Packet::read(&mut bytes, Some(100)).unwrap();
            assert!(matches!(decoded, Packet::Disconnect(p) if p.reason_code == reason));
        }
    }
    let packet = Packet::Disconnect(Disconnect {
        reason_code: DisconnectReasonCode::UnspecifiedError,
        properties: Some(DisconnectProperties {
            session_expiry_interval: Some(10),
            reason_string: Some("bye".into()),
            user_properties: vec![],
            server_reference: None,
        }),
    });
    let order = PropertyOrder {
        packet: vec![31, 17],
        will: vec![],
    };
    let mut bytes = BytesMut::new();
    packet
        .write_with_property_order(&mut bytes, None, &order)
        .unwrap();
    assert_eq!(
        Packet::read_with_property_order(&mut bytes, None).unwrap(),
        DecodedPacket {
            packet,
            property_order: order
        }
    );
}

#[test]
fn connect_and_will_property_orders_are_independent() {
    let packet = Packet::Connect(
        Connect {
            keep_alive: 30,
            client_id: "c".into(),
            clean_start: true,
            properties: Some(ConnectProperties {
                user_properties: vec![("u".into(), "1".into())],
                authentication_method: Some("SASL".into()),
                authentication_data: Some(Bytes::from_static(&[0, 255])),
                ..Default::default()
            }),
        },
        Some(LastWill {
            topic: Bytes::from_static(b"t"),
            message: Bytes::from_static(&[255]),
            qos: QoS::ExactlyOnce,
            retain: true,
            properties: Some(LastWillProperties {
                delay_interval: Some(1),
                payload_format_indicator: None,
                message_expiry_interval: None,
                content_type: Some("x".into()),
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            }),
        }),
        None,
    );
    let order = PropertyOrder {
        packet: vec![22, 38, 21],
        will: vec![3, 24],
    };
    let mut bytes = BytesMut::new();
    packet
        .write_with_property_order(&mut bytes, Some(4096), &order)
        .unwrap();
    assert_eq!(
        Packet::read_with_property_order(&mut bytes, Some(4096)).unwrap(),
        DecodedPacket {
            packet,
            property_order: order
        }
    );
}
