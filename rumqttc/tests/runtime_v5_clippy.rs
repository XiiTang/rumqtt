use rumqttc::v5::{
    mqttbytes::{v5::*, QoS},
    AsyncClient, Client, ClientError, Event, MqttState, Request,
};
use std::error::Error;

fn rejected_request(error: ClientError, expected: &Request, try_request: bool) {
    assert_eq!(
        error.to_string(),
        "Failed to send mqtt requests to eventloop"
    );
    assert!(error.source().is_none());
    let (request, actual_try_request): (&Request, bool) = match &error {
        ClientError::Request(request) => (request, false),
        ClientError::TryRequest(request) => (request, true),
    };
    assert_eq!(actual_try_request, try_request);
    assert_eq!(request, expected);
}

fn publication(topic: &str) -> Publish {
    let mut publication = Publish::new(
        topic,
        QoS::ExactlyOnce,
        b"payload".to_vec(),
        Some(PublishProperties {
            user_properties: vec![("key".into(), "value".into())],
            ..Default::default()
        }),
    );
    publication.retain = true;
    publication
}

#[test]
fn channel_errors_keep_the_complete_request_and_error_kind() {
    let request = Request::Publish(publication("fixture"));
    rejected_request(flume::SendError(request.clone()).into(), &request, false);
    for error in [
        flume::TrySendError::Full(request.clone()),
        flume::TrySendError::Disconnected(request.clone()),
    ] {
        rejected_request(error.into(), &request, true);
    }

    let (tx, rx) = flume::bounded(1);
    let client = Client::from_sender(tx);
    drop(rx);
    rejected_request(
        client.disconnect().unwrap_err(),
        &Request::Disconnect,
        false,
    );
    rejected_request(
        client.try_disconnect().unwrap_err(),
        &Request::Disconnect,
        true,
    );
}

#[tokio::test]
async fn validation_and_channel_failures_keep_their_existing_request_variants() {
    let (tx, rx) = flume::bounded(1);
    let client = AsyncClient::from_senders(tx);
    let invalid = publication("invalid/+");
    let expected = Request::Publish(invalid.clone());
    rejected_request(
        client
            .publish_with_properties(
                "invalid/+",
                invalid.qos,
                invalid.retain,
                invalid.payload.clone(),
                invalid.properties.clone().unwrap(),
            )
            .await
            .unwrap_err(),
        &expected,
        false,
    );
    rejected_request(
        client
            .try_publish_with_properties(
                "invalid/+",
                invalid.qos,
                invalid.retain,
                invalid.payload.clone(),
                invalid.properties.clone().unwrap(),
            )
            .unwrap_err(),
        &expected,
        true,
    );
    // This API already reports invalid topics as TryRequest; preserve that kind.
    rejected_request(
        client
            .publish_bytes_with_properties(
                "invalid/+",
                invalid.qos,
                invalid.retain,
                invalid.payload.clone(),
                invalid.properties.clone().unwrap(),
            )
            .await
            .unwrap_err(),
        &expected,
        true,
    );
    assert!(rx.is_empty());

    let valid = publication("fixture");
    let expected = Request::Publish(valid.clone());
    // A full channel and a disconnected one both retain the rejected request.
    client.try_disconnect().unwrap();
    rejected_request(
        client
            .try_publish_with_properties(
                "fixture",
                valid.qos,
                valid.retain,
                valid.payload.clone(),
                valid.properties.clone().unwrap(),
            )
            .unwrap_err(),
        &expected,
        true,
    );
    drop(rx);
    rejected_request(
        client
            .publish_with_properties(
                "fixture",
                valid.qos,
                valid.retain,
                valid.payload,
                valid.properties.unwrap(),
            )
            .await
            .unwrap_err(),
        &expected,
        false,
    );
}

#[test]
fn incoming_event_keeps_the_packet_and_accounts_for_its_box() {
    let mut state = MqttState::new(4, false);
    let before = state.retained_bytes();
    let mut publication = publication("fixture");
    publication.qos = QoS::AtMostOnce;
    let mut packet = Packet::Publish(publication);
    let bound = state.incoming_memory_bound(&packet);
    assert!(state.handle_incoming_packet(&mut packet).unwrap().is_none());
    assert_eq!(
        state.retained_bytes(),
        before + std::mem::size_of::<Packet>()
    );
    assert!(state.retained_bytes() <= bound);
    match state.events.front().unwrap() {
        Event::Incoming(incoming) => assert_eq!(incoming.as_ref(), &packet),
        event => panic!("Expected incoming packet, got {event:?}"),
    }
    state.events.clear();
    assert_eq!(state.retained_bytes(), before);
}
