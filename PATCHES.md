# IMAPipe integration patches

Base: rumqttc 0.25.1, upstream commit
`f1e9e8d558783f942993046679cdf3c8c3a3d36b`.
The fork preserves the upstream repository and Apache-2.0 licensing.

The MQTT 3.1.1 state machine can be driven through its existing packet interface
using an externally owned transport. IMAPipe does not use EventLoop reconnect or
clean/replay behavior. This patch keeps protocol state in MqttState and exposes
retained allocation, pending exchanges and manual acknowledgement selection.

Corrections in the same state machine:

- Correlate SUBACK/UNSUBACK with their request, including result count and granted
  QoS. Subscriptions, unsubscriptions, publications and releases share one packet
  identifier namespace. Generated identifiers skip pending requests.
- Track manual QoS 1/PUBREC boundaries, suppress duplicate QoS 2 application
  delivery, repeat the required PUBREC/PUBREL/PUBCOMP controls, and reject a PUBREL
  received before the application allowed PUBREC.
- Reject wrong-QoS acknowledgements without discarding the pending publication.
- Reject unsolicited PINGRESP and unsupported outgoing request variants.
- Validate explicit release bounds and do not count a repeated release twice.

The upstream PUBACK test previously acknowledged a QoS 2 publication with PUBACK;
its second publication is corrected to QoS 1. A new regression checks that the
wrong-QoS acknowledgement fails and leaves the QoS 2 exchange intact.

Validation is intentionally scoped: run rumqttc's library tests and
`tests/runtime_state.rs` with no default features, then IMAPipe's owned-peer and
isolated Mosquitto tests for both versions. No claim is made that the unused
reconnecting EventLoop or every optional transport has passed acceptance.
MQTT 5 state/codec integration and explicit session recovery remain separate work.

## MQTT 5 codec integration

The second increment enables AUTH in the top-level decoder and corrects its
binary/string lengths, multi-byte property length, and default short forms.
Every property decoder now receives an isolated exact-length section. Its one
parse loop enforces singleton/count bounds, preserves repeated identifiers, and
can record original property ordering alongside existing typed fields. No second
property parser was introduced. Control-property tails, canonical variable
integers, fixed-header flags, CONNACK flags and NUL strings are checked.

`Packet::read_with_property_order` and `read_frame_with_property_order` expose the
same decoder for Runtime evidence conversion. The latter accepts an immutable
frame so the caller can retain ownership, then reclaim and erase authentication
bytes after dropping all borrowed decoded fields. Numeric reason conversions use
the existing library tables. The public typed property structs are unchanged.

Eight fixed-wire and boundary regressions are in `tests/runtime_v5_codec.rs`.
The Runtime receive adapter uses this decoder and preserves original property
order; MQTT 5 outgoing encoding/state and explicit recovery are still pending.
