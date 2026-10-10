use crate::util::{self, GenericClientOpts, Harness};

use std::time::Duration;

use bytes::BytesMut;
use modular_mqtt::{ClientOpts, ClientOptsV3, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, ControlPacketType, MqttTopic, MqttV3_1_1,
    MqttV5_0_0, MqttVersion, Packet, PingReq, PingResp, Publish, Qos, QosPacketIdentifier,
    VersionedConnect,
};
use ntest::timeout;

async fn test_ping_sequence<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        let (header, mut data) = stream.read_packet().await;
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::<V>::new(true, 2, "client-id".to_string(), None, None, None).into()
        );
        stream
            .write_packet(|buf| ConnAck::<V>::new(false, connack_rc).write_to_buf(buf))
            .await;

        let (header, mut data) = stream.read_packet().await;
        assert_eq!(data.len(), 0);
        let connect = PingReq::try_read(header, &mut data).unwrap();
        assert_eq!(connect, PingReq);

        stream.write_packet(|buf| PingResp.write_to_buf(buf)).await;
        rx_close.recv().await;
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    H::sleep(Duration::from_millis(1500)).await;
    H::disconnect(client).await.unwrap();
    tx_close.send(()).await;
    handle.join().await;
}

test!(
    ping_sequence,
    test_ping_sequence,
    15000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

/// PINGREQ must arrive about once per keep_alive, not once per MIN_READ_TIMEOUT.                                                                                                                                                             
/// Reads many packets instead of one, so a flood cannot pass.
async fn test_ping_cadence<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        let (header, mut data) = stream.read_packet().await;
        VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        stream
            .write_packet(|buf| ConnAck::<V>::new(false, connack_rc).write_to_buf(buf))
            .await;

        let started = std::time::Instant::now();
        let mut pings = 0usize;
        while started.elapsed() < Duration::from_secs(6) {
            match stream.read_packet_timeout(Duration::from_millis(250)).await {
                None => continue,
                Some((header, _)) => {
                    assert_eq!(header.control_packet_type, ControlPacketType::PingReq);
                    pings += 1;
                    stream.write_packet(|buf| PingResp.write_to_buf(buf)).await;
                }
            }
        }
        // keep_alive = 2 over 6s => ~3 pings. Upper bound only, so CI jitter is safe;
        // the pre-fix flood produces hundreds.
        assert!(
            pings <= 4,
            "expected ~3 PINGREQs at keep_alive=2s, got {pings}"
        );
        rx_close.recv().await;
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    H::sleep(Duration::from_millis(6300)).await;
    H::disconnect(client).await.unwrap();
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    ping_cadence,
    test_ping_cadence,
    20000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

/// `keep_alive: 0` is legal (MQTT 3.1.1 §3.1.2.10) and means "no keepalive".
/// Must connect, deliver messages, and never send a PINGREQ.
async fn test_keep_alive_zero<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();
    let (tx_dc, mut rx_dc) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        let (header, mut data) = stream.read_packet().await;
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        // Advertised verbatim — catches a "fix" that clamps the field instead of mapping it.
        assert_eq!(
            match connect {
                VersionedConnect::V3(connect) => connect.keep_alive(),
                VersionedConnect::V5(connect) => connect.keep_alive(),
            },
            0
        );

        stream
            .write_packet(|buf| ConnAck::<V>::new(false, connack_rc).write_to_buf(buf))
            .await;

        stream
            .write_packet(|buf| {
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("t/ka0").unwrap(),
                    b"hello",
                    Qos::AtMostOnce,
                    false,
                )
                .assign_packet_identifier(|| 1, false)
                .write_to_buf(buf)
            })
            .await;

        // Sized for a tight loop (the broken behaviour pinged per iteration, ~400 in this
        // window), not for a correctly-paced ping.
        let msg = stream
            .read_packet_timeout(Duration::from_millis(2000))
            .await;
        assert_eq!(msg, None, "client sent a packet despite keep_alive=0");
        tx_dc.send(()).await;
        rx_close.recv().await;
    });

    let (mut rx, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 0,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    assert_eq!(
        *H::recv(&mut rx).await.payload(),
        bytes::Bytes::from_static(b"hello")
    );
    rx_dc.recv().await;
    H::disconnect(client).await.unwrap();
    tx_close.send(()).await;
    handle.join().await;
}

test!(
    keep_alive_zero,
    test_keep_alive_zero,
    10000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);
