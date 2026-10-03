macro_rules! test {
    (
    $mod:ident, $tc:ident, $timeout:literal,
    ($($($argsv3:expr)+$(,)?)*),
    ($($($argsv5:expr)+$(,)?)*)
    ) => {
        mod $mod {
            use super::*;
            #[test]
            #[ntest::timeout($timeout)]
            fn sync_v3() {
                futures::executor::block_on(
                    $tc::<crate::util::SyncHarness, modular_mqtt_protocol::MqttV3_1_1, modular_mqtt::ClientOptsV3>($($($argsv3)+),*)
                )
            }
            #[test]
            #[ntest::timeout($timeout)]
            fn sync_v5() {
                futures::executor::block_on(
                    $tc::<crate::util::SyncHarness, modular_mqtt_protocol::MqttV5_0_0, modular_mqtt::ClientOptsV5>($($($argsv5)+),*)
                )
            }

            #[cfg(feature="async")]
            #[tokio::test]
            async fn async_v3() {
                tokio::time::timeout(Duration::from_millis($timeout),
                    $tc::<crate::util::AsyncHarness, modular_mqtt_protocol::MqttV3_1_1, modular_mqtt::ClientOptsV3>( $($($argsv3)+),*)
                ).await.unwrap()
            }
            #[cfg(feature="async")]
            #[tokio::test]
            async fn async_v5() {
                tokio::time::timeout(Duration::from_millis($timeout),
                    $tc::<crate::util::AsyncHarness, modular_mqtt_protocol::MqttV5_0_0, modular_mqtt::ClientOptsV5>($($($argsv5)+),*)
                ).await.unwrap();
            }
        }
    };
}
mod connect;
mod ping;
mod publish;
// mod subscribe;
