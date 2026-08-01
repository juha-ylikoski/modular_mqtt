# modular-mqtt

An MQTT client for Rust supporting both **v3.1.1** and **v5.0.0**, with **sync** and **async**
(tokio) transports built on one shared client implementation. Unlike most MQTT clients, the goal
here is to expose the internals (inflight message state, reconnect behavior, error handling)
rather than hide them behind a black box.

> **⚠️ Work in progress.** This is under active development and not yet stable. Expect breaking
> API changes without notice, and check the [feature completeness](#feature-completeness) tables
> below before relying on this for anything that needs a specific v5 capability.

## Quick example

```rust,no_run
use std::time::Duration;
use modular_mqtt::{client::Client, client_opts::ClientOpts, connection::SyncWriter};
use modular_mqtt_protocol::{MqttV5_0_0, Publish, Qos};

let (recv_stream, client) = Client::<MqttV5_0_0, SyncWriter>::connect_tcp(
    ClientOpts { client_id: "my-client".to_string(), ..Default::default() },
    "127.0.0.1:1883".to_string(),
)?;

client.subscribe(vec!["some/topic".try_into()?], Qos::AtMostOnce, Duration::from_secs(5))?;
client.publish(Publish::new("some/topic".try_into()?, b"hello", Qos::AtMostOnce, false))?;

let msg = recv_stream.recv()?;
```

See `modular_mqtt/examples/` for full runnable versions (`connect`, `publish`, `subscribe` ×
v3/v5). Use the `async` feature (enabled by default) for the tokio-based client, or disable
default features for sync-only.

## Testing

- `cargo test -p modular-mqtt-protocol` — wire-format unit tests, no external dependencies.
- `cargo test -p modular-mqtt` — `tests/sync_tests/*` fake a broker with a raw `TcpListener`, no
  Docker needed; `tests/mosquitto.rs` spins up a real broker via `testcontainers` and needs Docker.
- `./mqtt_broker.sh` runs a local mosquitto broker for manually exercising the examples.
- `modular_mqtt_protocol/fuzz/` — coverage-guided fuzzing of the decoders via `cargo-fuzz` (see
  its own README for usage).

## Feature completeness

### MQTT v3.1.1

| Feature | Status | Notes |
|---|---|---|
| Connect/ConnAck (clean session, keep-alive, username/password, Last Will) | ✅ | |
| Publish QoS 0/1/2 | ✅ | inflight tracking + resend-on-timeout |
| Subscribe/Suback, Unsubscribe/Unsuback | ✅ | |
| Ping keep-alive | ✅ | |
| Retained-message flag (send + receive) | ✅ | |
| Client-initiated Disconnect | ✅ | |
| Wildcard topic filters in Subscribe (`+`, `#`) | ❌ | `MqttTopic` rejects `#`/`+` outright — no wildcard subscriptions are possible at all today |
| Graceful handling of broker-initiated Disconnect | ❌ | client panics (`OnDisconnectBehavior::Panic` is the only variant) |

### MQTT v5.0.0

All of the above, plus:

| Feature | Status | Notes |
|---|---|---|
| CONNECT v5 properties (session expiry, receive maximum, max packet size, topic alias maximum, request response/problem info, user properties, auth method/data) | ✅ | |
| Will v5 properties (delay interval, payload format, message expiry, content type, response topic, correlation data, user properties) | ✅ | |
| CONNACK v5 properties (server-advertised limits/capabilities) | ✅ | |
| PUBLISH v5 properties, receive side | ✅ | payload format, message expiry, topic alias, response topic, correlation data, user property, subscription identifier, content type |
| SUBSCRIBE v5 options (no-local, retain-as-published, retain handling, subscription identifiers, user properties) | ✅ | |
| PUBLISH topic alias, send side | ❌ | parsed on receive only; can't publish with an empty topic + alias |
| Shared subscriptions (`$share/group/topic`) | ❌ | |
| AUTH packet / enhanced re-authentication | ⚠️ | fully defined in the protocol crate, not wired into the client's connect flow |
| DISCONNECT v5 reason codes/properties surfaced to the caller | ❌ | client panics instead of exposing the reason |
| Receive Maximum flow-control enforcement | ❌ | sent in CONNECT, never enforced client-side |
| Server-provided keep-alive override | ❌ | parsed from CONNACK, not applied |
| Maximum packet size enforcement on send | ❌ | |
| Request/response correlation helper | ❌ | fields exist, no matching logic |
| Wildcard topic filters | ❌ | same root cause as v3.1.1 |

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
