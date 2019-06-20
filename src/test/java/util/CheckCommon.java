package util;

import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;
import util.client.MqttConnection;

@Slf4j
public class CheckCommon {

    public static void checkConnectionFail(MqttConnection connection, int... errorCode) {
        try {
            connection.connect();
        } catch (MqttException e) {
            log.info("Connect failed with mqtt exception as expected");
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

    public static void checkValidTopicPermission(MqttConnection connection, String pubTopic, String subTopic, int qos)
            throws Exception {
        String msg = "valid" + System.currentTimeMillis();
        int code = PubSubCommon.subscribeWithReturn(connection, subTopic, qos).getGrantedQos()[0];
        Assert.assertEquals("Reason code is wrong", qos, code);

        try {
            connection.getClient().publish(pubTopic, msg.getBytes(), qos, false);
        } catch (Exception e) {
            Assert.fail("publish error " + e);
        }

        Thread.sleep(1000);
        Assert.assertTrue("Connection is lost", connection.getClient().isConnected());
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