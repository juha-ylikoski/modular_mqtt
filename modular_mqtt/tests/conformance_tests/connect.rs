use crate::util::{self, write_packet, GenericClientOpts, Harness};

use std::{future::Future, net::TcpListener, pin::pin, time::Duration};

use bytes::Bytes;
use modular_mqtt::{error::ConnectError, ClientOpts, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, Disconnect, DisconnectReasonCode, MqttLastWill,
    MqttLastWill5_0_0, MqttTopic, MqttVersion, Packet, Publish, Qos, VersionedConnect,
};

async fn test_connect_no_server<H, V, O>()
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
{
    util::init_logging();
    let opts = GenericClientOpts(ClientOptsV5 {
        client_id: "client-id".to_string(),
        keep_alive: 30,
        ..Default::default()
    });
    match H::connect::<V, O>(opts, "127.0.0.1:1234".to_string()).await {
        Ok(_) => panic!("Should not happen"),
        Err(e) => match e {
            modular_mqtt::error::ConnectError::IoError(_) => (),
            _ => panic!("Should not happen"),
        },
    }
}
test!(connect_no_server, test_connect_no_server, 5000, (), ());

async fn test_connect<H, V, O>(connact_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::<V>::new(true, 30, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    connect,
    test_connect,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_connect_username_password<H, V, O>(connact_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(
                true,
                30,
                "".to_string(),
                None,
                Some("username".to_string()),
                Some(Bytes::from_static(b"password"))
            )
            .into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "".to_string(),
            keep_alive: 30,
            username: Some("username".to_string()),
            password: Some(Bytes::from_static(b"password")),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();

    handle.join().unwrap();
}
test!(
    connect_username_password,
    test_connect_username_password,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_connect_last_will<H, V, O>(connact_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(
                true,
                30,
                "".to_string(),
                Some(V::LastWill::new(
                    MqttTopic::try_from("last-will-topic").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::AtMostOnce,
                    false,
                )),
                None,
                None,
            )
            .into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "".to_string(),
            keep_alive: 30,
            will: Some(MqttLastWill5_0_0::new(
                MqttTopic::try_from("last-will-topic").unwrap(),
                Bytes::from_static(b"payload"),
                Qos::AtMostOnce,
                false,
            )),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    connect_last_will,
    test_connect_last_will,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_connect_refused<H, V, O>(connact_rc: V::ConnackRc, tester: impl FnOnce(ConnectError))
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(true, 30, "".to_string(), None, None, None,).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
    });

    match H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    {
        Ok(_) => panic!("Should not get here"),
        Err(e) => tester(e),
    }
    handle.join().unwrap();
}
test!(
    connect_refused,
    test_connect_refused,
    5000,
    (ConnectRcV3::Refused, |e| match e {
        modular_mqtt::error::ConnectError::ConnectFailedV3(connect_rc) => {
            assert_eq!(connect_rc, ConnectRcV3::Refused)
        }
        _ => panic!("Should not get here"),
    }),
    (ConnectRcV5::NotAuthorized, |e| match e {
        modular_mqtt::error::ConnectError::ConnectFailedV5(connect_rc) => {
            assert_eq!(connect_rc, ConnectRcV5::NotAuthorized)
        }
        _ => unreachable!(),
    })
);

async fn test_disconnect<H, V, O>(connact_rc: V::ConnackRc, disconnect: Disconnect<V>)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(true, 30, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf);
            disconnect.write_to_buf(buf);
        });
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    handle.join().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while H::online(&client) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!H::online(&client));
    H::publish(
        &client,
        Publish::new("foo".try_into().unwrap(), b"bar", Qos::AtMostOnce, false),
    )
    .await
    .unwrap();
}

mod disconnect {
    use modular_mqtt::ClientOptsV3;

    use super::*;
    #[test]
    #[ntest::timeout(5000)]
    #[should_panic]
    fn v3() {
        futures::executor::block_on(test_disconnect::<
            crate::util::SyncHarness,
            modular_mqtt_protocol::MqttV3_1_1,
            ClientOptsV3,
        >(ConnectRcV3::Accepted, Disconnect::new_v3()))
    }
    #[test]
    #[ntest::timeout(5000)]
    #[should_panic]
    fn v5() {
        futures::executor::block_on(test_disconnect::<
            crate::util::SyncHarness,
            modular_mqtt_protocol::MqttV5_0_0,
            ClientOptsV5,
        >(
            ConnectRcV5::Accepted,
            Disconnect::new_v5(DisconnectReasonCode::Normal, None, None, Vec::new(), None),
        ))
    }
}
