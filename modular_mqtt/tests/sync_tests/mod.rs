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
            fn v3() {
                $tc::<modular_mqtt_protocol::MqttV3_1_1, modular_mqtt::ClientOptsV3>($($($argsv3)+),*)
            }
            #[test]
            #[ntest::timeout($timeout)]
            fn v5() {
                $tc::<modular_mqtt_protocol::MqttV5_0_0, modular_mqtt::ClientOptsV5>($($($argsv5)+),*)
            }
        }
    };
}
mod connect;
mod ping;
mod publish;
mod subscribe;
