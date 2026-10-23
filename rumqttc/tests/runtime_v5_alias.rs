use rumqttc::v5::{
    mqttbytes::{v5::*, QoS},
    Event, MqttState,
};

#[test]
fn incoming_packets_keep_the_resolved_topic_before_duplicate_suppression() {
    for manual in [false, true] {
        let mut state = MqttState::new(4, manual);
        state.configure_receive(4, 1, 1024, true).unwrap();
        // The first packet assigns the alias. Reassign it on a duplicate, then
        // use only that alias: resolution must precede duplicate suppression.
        for (topic, expected, duplicate) in [
            ("sensors/1", "sensors/1", false),
            ("", "sensors/1", true),
            ("sensors/2", "sensors/2", true),
            ("", "sensors/2", true),
        ] {
            let mut publish = Publish::new(
                topic,
                QoS::ExactlyOnce,
                b"payload".to_vec(),
                Some(PublishProperties {
                    topic_alias: Some(1),
                    ..Default::default()
                }),
            );
            publish.pkid = 7;
            publish.dup = duplicate;
            let mut packet = Packet::Publish(publish);
            state.handle_incoming_packet(&mut packet).unwrap();
            assert!(
                matches!(&packet, Packet::Publish(p) if p.topic.as_ref() == expected.as_bytes())
            );
            assert_eq!(
                state
                    .events
                    .drain(..)
                    .filter(|e| matches!(e, Event::Incoming(_)))
                    .count(),
                usize::from(!duplicate)
            );
        }
    }
}
