package edgeTestCases.utils;

import edgeTestCases.utils.client.MqttConnection;
import edgeTestCases.utils.client.RandomNameHolder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;


@Slf4j
public class CheckCommon {

    public static void checkMapEquals(Map<String, Object> expectedMap, Map<String, Object> actualMap) throws Exception {
        Assert.assertEquals("Key is wrong", expectedMap.keySet(), actualMap.keySet());
        checkMapContains(expectedMap, actualMap);
    }

    public static void checkMapContains(Map<String, Object> expectedMap, Map<String, Object> actualMap,
                                        boolean... equals) throws Exception {
        for (String key : expectedMap.keySet()) {
            try {
                if (expectedMap.get(key) instanceof List) {
                    List expectList = (List) expectedMap.get(key);
                    List actualList = (List) actualMap.get(key);
                    Assert.assertEquals("The list size doesn't match", expectList.size(), actualList.size());
                    for (int index = 0; index < expectList.size(); index++) {
                        Map<String, Object> expectedMapAgain = Converter.modelToMap(expectList.get(index));
                        Map<String, Object> actualMapAgain = Converter.modelToMap(actualList.get(index));
                        log.info("Key {} is in map format", key);
                        if (equals.length > 0 && !equals[0]) {
                            checkMapContains(expectedMapAgain, actualMapAgain);
                        } else {
                            checkMapEquals(expectedMapAgain, actualMapAgain);
                        }
                    }
                    continue;
                }
                Map<String, Object> expectedMapAgain = Converter.modelToMap(expectedMap.get(key));
                Map<String, Object> actualMapAgain = Converter.modelToMap(actualMap.get(key));
                log.info("Key {} is in map format", key);
                if (equals.length > 0 && !equals[0]) {
                    checkMapContains(expectedMapAgain, actualMapAgain);
                } else {
                    checkMapEquals(expectedMapAgain, actualMapAgain);
                }
            } catch (Exception e) {
                String expectedValue = RandomNameHolder.getNumberOfScientificString(String.valueOf(expectedMap
                        .get(key)));
                String actualValue = RandomNameHolder.getNumberOfScientificString(String.valueOf(actualMap.get(key)));
                Assert.assertEquals(String.format("Key %s value is wrong", key), expectedValue, actualValue);
            }
        }
    }

    public static void checkConnectionFail(MqttConnection connection, int... errorCode) {
        try {
            connection.connect();
        } catch (MqttException e) {
            log.info("Connect failed with edgeTestCases.mqtt exception as expected");
            if (errorCode.length > 0) {
                Assert.assertEquals("Error code is wrong", errorCode[0], e.getReasonCode());
            }
        } catch (Exception e) {
            log.info("Connect failed as expected");
        }
        Assert.assertFalse("Connection should not connect", connection.getClient().isConnected());
        // Paho may retry after connection fail
        try {
            PubSubCommon.stopConnectionForciblyWithoutWait(connection);
        } catch (Exception e) {
            log.error("Forcibly stop connection fail.", e);
        }
    }

    public static void checkValidTopicPermission(MqttConnection connection, String topic, int qos) throws Exception {
        // Check sub
        int code = PubSubCommon.subscribeWithReturn(connection, topic, qos).getGrantedQos()[0];
        Assert.assertEquals("Reason code is wrong", qos, code);

        // Check pub
        String msg = "valid" + System.currentTimeMillis();
        PubSubCommon.publish(connection, topic, qos, msg, false);
        PubSubCommon.checkPubAndSubResult(Collections.singletonList(msg), connection.getCallback()
                .waitAndGetReveiveList(), qos);
    }

    public static void checkIllegalTopicPermission(MqttConnection connection, String topic, int qos) throws Exception {
        // Check sub
        checkIllegalTopicSubPermission(connection, topic, qos);

        // Check pub
        checkIllegalTopicPubPermission(connection, topic, qos);
    }

    public static void checkIllegalTopicSubPermission(MqttConnection connection, String topic, int qos)
            throws Exception {
        try {
            int code = PubSubCommon.subscribeWithReturn(connection, topic, qos).getGrantedQos()[0];
            Assert.assertEquals("Reason code is wrong", MqttException.REASON_CODE_SUBSCRIBE_FAILED, code);
        } catch (IllegalArgumentException e) {
            log.error("Illegal arg exception thrown by paho lib", e);
        }
    }

    public static void checkIllegalTopicPubPermission(MqttConnection connection, String topic, int qos)
            throws Exception {
        try {
            connection.getClient().publish(topic, "test".getBytes(), qos, false);
        } catch (IllegalArgumentException e) {
            log.error("Illegal arg exception thrown by paho lib", e);
            connection.disconnect(); // For finally
        } catch (Exception e) {
            log.error("Connection disconnect with illegal pub as expected");
        } finally {
            Thread.sleep(1000);
            Assert.assertFalse("Connection should be lost", connection.getClient().isConnected());
        }
    }

    public static void checkDevicesHavingNoPubAuthority(List<MqttConnection> devices, String topic) throws Exception {
        for (MqttConnection connection : devices) {
            if (!connection.getClient().isConnected()) {
                connection.connect();
            }
            PubSubCommon.publish(connection, topic, 0, "test", false);
        }
        Thread.sleep(1000);
        for (MqttConnection connection : devices) {
            Assert.assertFalse("Connection should be lost", connection.getClient().isConnected());
        }
    }
}