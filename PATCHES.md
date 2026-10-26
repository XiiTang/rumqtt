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

## Admission and header integration

`MqttState::initial_memory_bound` exposes a conservative allocation bound before
constructing state or opening a transport. Boundary tests compare it with actual
retained allocations. The Runtime owns the corresponding reservation throughout
the state lifetime and rejects insufficient budgets before network dispatch.

`FixedHeader::parse` exposes the existing MQTT 5 header validation and its length
fields so transport framing does not duplicate packet flag and QoS rules.

## MQTT 5 ordered sending

`Packet::write_with_property_order` sends the existing typed packet through one
shared property encoder. It consumes repeated values in order and rejects an
inconsistent property inventory before exposing a partial frame. CONNECT and its
Will have independent property order; no encoded frame is reparsed to reorder it.
The former per-structure property writers now delegate to this same encoder.
CONNECT preserves empty credential fields and binary passwords and correctly
appends to nonempty output buffers. DISCONNECT uses the correct remaining length
for both short forms and empty/nonempty property sections.

Validation: 73 library tests, 7 state regressions, 8 receive-codec regressions and
5 writer regressions passed. IMAPipe's 17 MQTT tests (including isolated Mosquitto)
passed after its outgoing codec was replaced with this library entry point.
MQTT 5 state integration and explicit recovery remain separate required work.

## MQTT 5 externally driven state

The v5 `MqttState` now owns publication, subscription and unsubscription packet
identifiers in one namespace; correlates acknowledgement kinds, subscription
counts and granted QoS; and preserves negative terminal outcomes without replay.
Incoming QoS 2 duplicates retain the manual acknowledgement boundary. The caller
may defer receive-credit release until the emitted acknowledgement has actually
been written, using a generation token that cannot clear a reused packet ID.
Topic aliases use bounded owned topic bytes, independent of the received frame.

The Runtime can inspect initial, retained and next-transition memory bounds before
allocating or changing state. These bounds include collection growth and property
allocations. Exhausted send capacity is an explicit error. The Runtime does not
use the reconnecting EventLoop or implicit replay helpers. Explicit session
recovery remains a separate required integration.

Validation: 73 library tests, 7 MQTT 3 state regressions, 8 MQTT 5 receive-codec
regressions, 5 MQTT 5 writer regressions and 7 MQTT 5 state/admission regressions
passed. All 17 Runtime MQTT tests passed, including isolated Mosquitto for both
versions, manual acknowledgement, negative QoS 2, frozen authentication,
reauthentication privacy, cancellation and resource release.

## Fixed certificate verification in the 0.103 TLS graph (#312)

Update the workspace lockfile's rustls-webpki 0.103.8 to 0.103.16 for
RUSTSEC-2026-0382. webpki requires aws-lc-rs 1.18; keep its minimum
1.18.0 and aws-lc-sys 0.44.0, removing the old native generator dependencies
that are no longer required. Retain other locked versions. No MQTT state,
codec, recovery or transport implementation changes. Regression coverage:
the rumqttc library and the runtime_state, runtime_v5_state, runtime_v5_codec
and runtime_v5_writer integration suites, plus Boundless's full MQTT module.

## Resolved incoming MQTT 5 topics (#167)

`MqttState::handle_incoming_packet` takes the caller's packet by mutable
reference. Its existing private PUBLISH handler resolves the connection's
Topic Alias before QoS handling; that resolved topic now stays in the caller's
packet even when a QoS 2 duplicate emits no `Event::Incoming`. The EventLoop,
framed reader and state tests use this same entry point. The alias table stays
private; no second resolver or owned-packet compatibility entry point is added.

Reason: Boundless previously obtained a resolved topic only from an Incoming
event, so an alias-only QoS 2 duplicate reached its program with an empty topic.
Regression: `runtime_v5_alias::incoming_packets_keep_the_resolved_topic_before_duplicate_suppression`
checks automatic and manual acknowledgement, alias-only duplicates and alias
reassignment on a duplicate. Boundless's independent-byte peer test
`issue_167_a_duplicate_named_by_its_alias_carries_the_topic` checks the actual
program-visible topic and the peer's PUBREC in both acknowledgement modes.

## Strict Clippy on Rust 1.97 (#320)

Reproduced on Boundless's pin `11ca1518d43f8ecc6fb84dbad3dca3c8cb6fe469`:
`52bf09b1fee92cf29edd007a5d78c335ffa70d2f` has the identical source tree and
only adds history-preservation merges. On macOS arm64 with Rust 1.97.0,
`cargo clippy -p rumqttc --lib --tests --locked --no-default-features -- -D warnings`
exited 101: 38 `result_large_err`, one `large_enum_variant`, ten
`incompatible_msrv`, two `derivable_impls`, and seven
`items_after_test_module` diagnostics. All 58 issue findings reproduced. Once these library diagnostics were fixed,
the same command exited 101 with three newly reached `unused_must_use`
warnings in `runtime_v5_state`: the tests ignored `resume_session` results.
They now assert successful recovery with `unwrap`, as the existing state
tests do, without changing production recovery or hiding an error.

Keep upstream's declared Rust 1.64.0 MSRV. Replace `is_some_and` with
`map_or(false, ...)`, bounded integer `div_ceil` with the same arithmetic
already used by the MQTT 5 allocation bound, and `let ... else` with `match`.
The ceiling expression cannot overflow: its input is a `u16` plus one.
Enum `Default` derives and `#[default]` are available since Rust 1.62.
No newer Rust feature is needed by this repair, so no MSRV increase is warranted.
The existing lockfile resolves newer upstream dependencies (including Tokio
1.48 requiring Rust 1.71); it is not a Rust 1.64 dependency fixture. This patch
preserves that graph and makes no claim of a complete locked build on 1.64.

Box the MQTT 5 `ClientError::{Request, TryRequest}` payloads, retaining the
complete rejected request, original error variant, display text and source.
Box `v5::Event::Incoming` and count its packet allocation in `retained_bytes`.
The queued packet, event order and duplicate suppression stay unchanged. Users
matching the payload use the box's reference or dereference it; the MQTT 5
manual-acknowledgement example and state regression are updated accordingly.
The two existing enum defaults are derived with the same default variants.
The seven reason-code conversions move before their test modules unchanged.
No lint suppression is added. Upstream HEAD
`e886a788935d1b78a666b45124b9036cd6d14002` was reviewed: it still uses the
inline MQTT 5 error/event payloads and manual enum defaults, and has no newer
repair to transplant for these findings. Existing fork code already uses boxed
packets for `ConnectionError::NotConnAck`.

The shared error conversions and direct client rejection sites are the error
representation boundary; `MqttState::handle_incoming_packet` is the single
MQTT 5 incoming-event construction point. The `runtime_v5_clippy` regressions
cover full/disconnected channel errors, synchronous and asynchronous clients,
invalid-topic rejection (including the existing bytes-API variant), full
request contents, and an incoming event's unchanged packet and accounted box.
Existing MQTT 3/5 state, codec, writer and alias regressions cover the other paths.

Validation after the repair on Rust 1.97.0, macOS arm64:

- `cargo clippy -p rumqttc --lib --tests --locked --no-default-features -- -D warnings`: exit 0 (the same command that exited 101 before the repair).
- `cargo test -p rumqttc --lib --test runtime_state --test runtime_v5_state --test runtime_v5_codec --test runtime_v5_writer --test runtime_v5_alias --test runtime_v5_clippy --locked --no-default-features`: exit 0; 113 passed, zero failed.
- `cargo check -p rumqttc --example async_manual_acks_v5 --locked --no-default-features`: exit 0.
- `cargo fmt -p rumqttc -- --check`: exit 0.

All compilation and tests used the shared build-slot wrapper with incremental
compilation disabled. The reconnecting broker/reliability integration suites
and optional TLS/websocket/proxy feature combinations were not exercised.

## Passive publication retirement facts (W14a)

Explicit session recovery already removes never-transmitted publications and MQTT 5
replay already removes expired QoS1 publications. A caller owning retained publication
outcomes otherwise receives no native completion for these removals.
`MqttState::take_publication_retirements` (both versions) passively drains typed
NotTransmitted / MessageExpired facts emitted only where those existing removals
occur. MessageExpired retains the native expiry interval; no payload, timer, new
expiry rule or retry is added. The caller drains after resume and every replay call,
including None. The private native removal branches are the emission checkpoint;
callers cannot insert a fact. The drain transfers its accounted Vec backing.

Regression coverage: state.rs unsent removal/drain backing (F1/F5); v5/state.rs
unsent removal, Some/None replay expiry and drain backing (F2/F3/F4/F5), plus preserved
sent-QoS2 recovery. Boundless adapter T16/T20 coverage belongs to the pin integration.

Validation on macOS arm64, Rust 1.97.0, based on `111f5ddac9b77c8f493a5595607a262cd7fd654e`:

- `cargo +1.97.0 test -p rumqttc --lib --test runtime_state --test runtime_v5_state --locked --no-default-features retirement`: exit 0, six library regressions; integration tests filtered out.
- `cargo +1.97.0 test -p rumqttc --lib --test runtime_state --test runtime_v5_state --locked --no-default-features recovery`: exit 0; scoped recovery regressions only.
- `cargo +1.97.0 clippy -p rumqttc --lib --tests --locked --no-default-features -- -D warnings`: exit 0.

All compilation/tests used build-slot with incremental compilation disabled.
Optional transports, broker interoperability and the reconnecting EventLoop were not run.
