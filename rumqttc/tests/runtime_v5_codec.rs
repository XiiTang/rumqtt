use bytes::{Bytes, BytesMut};
use rumqttc::v5::mqttbytes::v5::*;

fn decode(wire: &[u8]) -> Packet {
    let mut input = BytesMut::from(wire);
    let packet = Packet::read(&mut input, Some(65536)).unwrap();
    assert!(input.is_empty());
    packet
}

#[test]
fn auth_fixed_wire_preserves_binary_data_and_actual_lengths() {
    let wire = b"\xf0\x0e\x18\x0c\x15\0\x04SASL\x16\0\x02\0\xff";
    let expected = Packet::Auth(Auth {
        code: AuthReasonCode::Continue,
        properties: Some(AuthProperties {
            method: Some("SASL".into()),
            data: Some(Bytes::from_static(b"\0\xff")),
            ..Default::default()
        }),
    });
    assert_eq!(decode(wire), expected);
    let mut encoded = BytesMut::new();
    assert_eq!(
        expected.write(&mut encoded, Some(65536)).unwrap(),
        wire.len()
    );
    assert_eq!(expected.size(), wire.len());
    assert_eq!(&encoded[..], wire);
}

#[test]
fn auth_and_disconnect_allow_omitted_default_fields() {
    assert!(matches!(
        decode(b"\xf0\0"),
        Packet::Auth(Auth {
            code: AuthReasonCode::Success,
            properties: None
        })
    ));
    assert!(matches!(
        decode(b"\xf0\x01\x18"),
        Packet::Auth(Auth {
            code: AuthReasonCode::Continue,
            properties: None
        })
    ));
    assert!(
        matches!(decode(b"\xe0\0"), Packet::Disconnect(d) if d.reason_code == DisconnectReasonCode::NormalDisconnection)
    );
    assert!(
        matches!(decode(b"\xe0\x01\x04"), Packet::Disconnect(d) if d.reason_code == DisconnectReasonCode::DisconnectWithWillMessage)
    );
    let mut bytes = BytesMut::new();
    let packet = Packet::Auth(Auth {
        code: AuthReasonCode::Success,
        properties: None,
    });
    assert_eq!(packet.write(&mut bytes, None).unwrap(), 2);
    assert_eq!(&bytes[..], b"\xf0\0");
}

#[test]
fn repeated_subscription_identifiers_do_not_consume_payload() {
    let Packet::Publish(p) = decode(b"\x30\x0b\0\x01a\x05\x0b\x01\x0b\x80\x01\xaa\xbb") else {
        panic!()
    };
    assert_eq!(p.properties.unwrap().subscription_identifiers, vec![1, 128]);
    assert_eq!(&p.payload[..], b"\xaa\xbb");
}

#[test]
fn property_sections_cannot_borrow_payload_or_overwrite_singletons() {
    for wire in [
        &b"\x30\x06\0\x01a\x01\x0b\x01"[..], // property value is outside the section
        &b"\x30\x08\0\x01a\x04\x01\0\x01\0"[..], // duplicate singleton
        &b"\x20\x04\0\0\0\xff"[..],          // bytes after CONNACK properties
        &b"\xf0\x03\x18\0\xff"[..],          // bytes after AUTH properties
        &b"\x40\x05\0\x01\0\0\xff"[..],      // bytes after PUBACK properties
        &b"\x50\x05\0\x01\0\0\xff"[..],
        &b"\x62\x05\0\x01\0\0\xff"[..],
        &b"\x70\x05\0\x01\0\0\xff"[..],
        &b"\xe0\x03\0\0\xff"[..],
    ] {
        assert!(
            Packet::read(&mut BytesMut::from(wire), Some(65536)).is_err(),
            "{wire:02x?}"
        );
    }
}

#[test]
fn auth_multibyte_property_length_and_property_count_are_bounded() {
    let packet = Packet::Auth(Auth {
        code: AuthReasonCode::ReAuthenticate,
        properties: Some(AuthProperties {
            method: Some("SASL".into()),
            data: Some(Bytes::from(vec![0xff; 128])),
            reason: Some("challenge".into()),
            user_properties: vec![("key".into(), "value".into())],
        }),
    });
    let mut wire = BytesMut::new();
    assert_eq!(packet.write(&mut wire, None).unwrap(), packet.size());
    assert_eq!(wire.len(), packet.size());
    assert_eq!(decode(&wire), packet);
    let huge = Packet::Auth(Auth {
        code: AuthReasonCode::Continue,
        properties: Some(AuthProperties {
            user_properties: vec![(String::new(), String::new()); 1025],
            ..Default::default()
        }),
    });
    wire.clear();
    assert!(huge.write(&mut wire, None).is_err());
    // Independent malformed frame: 1025 empty user-property pairs.
    wire = BytesMut::from(&[0xf0, 0x88, 0x28, 0x18, 0x85, 0x28][..]);
    for _ in 0..1025 {
        wire.extend_from_slice(&[38, 0, 0, 0, 0]);
    }
    assert!(Packet::read(&mut wire, Some(65536)).is_err());
}

#[test]
fn fixed_flags_and_nonminimal_variable_integers_are_rejected() {
    for wire in [
        &b"\xf1\0"[..],
        &b"\xe1\0"[..],
        &b"\xc2\0"[..],
        &b"\xd0\x01\0"[..],
        &b"\xf0\x80\0"[..],
    ] {
        assert!(Packet::read(&mut BytesMut::from(wire), None).is_err());
    }
}

#[test]
fn property_order_is_recorded_during_the_same_decode() {
    // Repeated user properties straddle two different singleton properties.
    let wire = b"\xf0\x16\x18\x14\x26\0\x01a\0\x01b\x15\0\x01m\x26\0\x01c\0\x01d\x16\0\0";
    // Correct remaining/property lengths are derived independently below from
    // the literal's byte count; no library encoder participates in the fixture.
    let mut bytes = wire.to_vec();
    bytes[1] = (bytes.len() - 2) as u8;
    bytes[3] = (bytes.len() - 4) as u8;
    let frame = Bytes::from(bytes);
    let result = Packet::read_frame_with_property_order(frame.clone(), None).unwrap();
    assert_eq!(result.property_order.packet, vec![38, 21, 38, 22]);
    assert!(result.property_order.will.is_empty());
    let Packet::Auth(auth) = result.packet else {
        panic!()
    };
    assert_eq!(
        auth.properties.unwrap().user_properties,
        vec![("a".into(), "b".into()), ("c".into(), "d".into())]
    );
    // All borrowed decoder fields were dropped, so the caller can erase this
    // original allocation without making another copy of authentication bytes.
    assert!(frame.try_into_mut().is_ok());
}

#[test]
fn connack_flags_and_nul_strings_are_rejected() {
    for wire in [
        &b"\x20\x03\x02\0\0"[..],
        &b"\x20\x03\x01\x80\0"[..],
        &b"\xf0\x06\x18\x04\x15\0\x01\0"[..],
    ] {
        assert!(Packet::read(&mut BytesMut::from(wire), None).is_err());
    }
}
