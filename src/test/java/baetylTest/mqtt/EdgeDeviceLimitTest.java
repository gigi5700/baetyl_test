package baetylTest.mqtt;

import baetylTest.EDGEIntegrationTest;
import baetylTest.utils.CheckCommon;
import baetylTest.utils.PubSubCommon;
import baetylTest.utils.client.ConnectionType;
import baetylTest.utils.client.MqttConnection;
import baetylTest.utils.client.PubSubCallback;
import baetylTest.utils.client.RandomNameHolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for mqtt device limit checker
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceLimitTest extends EDGEIntegrationTest {

    public ConnectionType connectionType;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions connectOptions;

    @Before
    public void setUp() throws Exception {
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL
                .equals(connectionType) ? true : false;
        qos = random.nextInt(2);
        log.info("Connection type is {}", connectionType.toString());

        connectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
    }
    
    /**
     * TestGoal: Test payload size limit checker
     *
     * @throws Exception
     */
    @Test
    public void testPayloadLimit() throws Exception {
        String topic = "payload/length/" + System.currentTimeMillis();

        // Change protocol if is ws or wss
        if (connectionType.equals(ConnectionType.WS) || connectionType.equals(ConnectionType.WSS)) {
            connectionType = ConnectionType.TCP;
            tls = false;
            log.info("Changing type to {}", connectionType.toString());
        }

        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        MqttConnection sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Sub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        sub.setCallBack(new PubSubCallback());
        try {
            pub.connect();
            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);

            // Check packet size now, maybe small than limit
            String correctMsg = RandomNameHolder.getRandomNumberString(payloadLengthLimit - 1024);
            log.info("msg size {}", correctMsg.getBytes().length);
            PubSubCommon.publish(pub, topic, qos, correctMsg, false);
            PubSubCommon.checkPubAndSubResult(Collections.singletonList(correctMsg), sub.getCallback()
                    .waitAndGetReveiveList(), qos);

            String message = RandomNameHolder.getRandomNumberString(payloadLengthLimit + 1);
            PubSubCommon.publish(pub, topic, qos, message, false);
            Thread.sleep(SLEEP_TIME);
            Assert.assertFalse("Connection should lost", pub.getClient().isConnected());
            Assert.assertTrue("Payload filter doesn't work", sub.getCallback().getReceiveList().size() == 0);
        } catch (Exception e) {
            throw e;
        } finally {
            pub.disconnect();
            sub.disconnect();
        }
    }
    
    /**
     * TestGoal: Test client id length limit checker
     *
     * @throws Exception
     */
    @Test
    public void testClientIdLength() throws Exception {
        List<String> clientIds = Arrays.asList(RandomNameHolder.getRandomString(clientIdLengthLimit - 1),
                RandomNameHolder.getRandomString(clientIdLengthLimit));
        for (String clientId : clientIds) {
            MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                    connectionType, offlineEdgePortMap), clientId, tls, edgeCertPath, connectOptions);
            connection.connect();
            Assert.assertTrue(String.format("Connect failed [%s]", clientId), connection.getClient().isConnected());
            connection.disconnect();
        }

        String clientId = RandomNameHolder.getRandomString(clientIdLengthLimit + 1);
        MqttConnection connection = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap), clientId, tls,
                edgeCertPath, connectOptions);
        try {
            CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_INVALID_CLIENT_ID);
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }
    
    @Test
    public void testConnectWithEmptyClientId() throws Exception {
        // Feature of 3.1.1
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "", tls, edgeCertPath, connectOptions);
        connection.connect();
        Assert.assertTrue("Connect failed with empty client id", connection.getClient().isConnected());
        connection.disconnect();
        
        MqttConnectOptions optionsWithCleanSessionFalse = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                offlineEdgePassword);
        optionsWithCleanSessionFalse.setCleanSession(false);
        connection.setConnOpts(optionsWithCleanSessionFalse);
        try {
            CheckCommon.checkConnectionFail(connection);
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }
    
    /**
     * TestGoal: Test feature of validation of client id
     *
     * @throws Exception
     */
    @Test
    public void testClientIdCheck() throws Exception {
        List<String> validClientIds = Arrays.asList(
                String.format("%s%s_%s-", RandomNameHolder.getRandomString(3).toUpperCase(), RandomNameHolder
                        .getRandomString(3).toLowerCase(), RandomNameHolder.getRandomNumberString(3)),
                RandomNameHolder.getRandomNumberString(5));
        List<String> invalidClientIds = Arrays.asList(RandomNameHolder.getRandomInvalidString(5), "中文");

        MqttConnection connection = null;
        for (String clientId : validClientIds) {
            connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                    connectionType, offlineEdgePortMap), clientId, tls, edgeCertPath, connectOptions);
            connection.connect();
            Assert.assertTrue(String.format("Connect failed [%s]", clientId), connection.getClient().isConnected());
            connection.disconnect();
        }     

        for (String clientId : invalidClientIds) {
            connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                    connectionType, offlineEdgePortMap), clientId, tls, edgeCertPath, connectOptions);
            try {
                CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_INVALID_CLIENT_ID);
            } catch (Exception e) {
                throw e;
            } finally {
                connection.disconnect();
            }
        }    
    }
    
    /**
     * TestGoal: Test topic length limit checker
     *
     * @throws Exception
     */
    @Test
    public void testTopicLengthCheck() throws Exception {
        // Subscibing illegal topic in running time will get no exception but error in suback
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "topicLength" + System.currentTimeMillis(), tls, edgeCertPath,
                connectOptions);
        connection.setCallBack(new PubSubCallback());
        connection.connect();
        List<String> validTopics = Arrays.asList(
                RandomNameHolder.getRandomString(topicLengthLimit - 1),
                RandomNameHolder.getRandomString(random.nextInt(topicLengthLimit - 1) + 1),
                RandomNameHolder.getRandomString(topicLengthLimit));
        for (String topic : validTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkValidTopicPermission(connection, topic, qos);
        }
        
        List<String> invalidTopics = Arrays.asList("", RandomNameHolder.getRandomString(topicLengthLimit + 1));
        for (String topic : invalidTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkIllegalTopicPermission(connection, topic, qos);
            connection.connect();
        }
    }
    
    /**
     * TestGoal: Test slash count limit checker for topic
     *
     * @throws Exception
     */
    @Test
    public void testSlashLimitInTopic() throws Exception {
        // Subscibing illegal topic in running time will get no exception but error in suback
        List<String> validTopics = Arrays.asList("1/2/3/4/5/6/7/8", "1/2/3/4/5/6/7/8/9");
        List<String> invalidTopics = Arrays.asList("1/2/3/4/5/6/7/8/9/10");
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "topicSlash" + System.currentTimeMillis(), tls, edgeCertPath,
                connectOptions);
        connection.setCallBack(new PubSubCallback());
        connection.connect();
        for (String topic : validTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkValidTopicPermission(connection, topic, qos);
        }
        
        for (String topic : invalidTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkIllegalTopicPermission(connection, topic, qos);
            connection.connect();
        }
    }
    
    /**
     * TestGoal: Test feature of validation of topic
     *
     * @throws Exception
     */
    @Test
    public void testTopicValidation() throws Exception {
        // U+0000, Empty
        char[] emtpyCode = new char[1];
        emtpyCode[0] = '\u0000';
        List<String> invalidTopics = Arrays.asList(new String(emtpyCode), "", "$123", "$baetylTest.function/123");
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "topicValidation" + System.currentTimeMillis(), tls, edgeCertPath,
                connectOptions);
        connection.setCallBack(new PubSubCallback());
        connection.connect();
        
        for (String topic : invalidTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkIllegalTopicPermission(connection, topic, qos);
            connection.connect();
        }
    }
    
    /**
     * TestGoal: Test feature of validation of topics containing wildcard characters
     *
     * @throws Exception
     */
    @Test
    public void testTopicValidationForWildcard() throws Exception {
        List<String> invalidTopics = Arrays.asList("++", "##", "+a/b", "a/+b", "a/+b/c", "++/a/b", "a/++/b", "a/b/++",
                "#/a", "a/#/b", "#/a/#/b", "##/ab", "a/##/b", "a/b/##", "#/a/+", "#+/a/b", "+#/a/b", "a/+#/b",
                "a/#+/b", "a/b/+#", "a/b/#+");
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "topicValidation" + System.currentTimeMillis(), tls, edgeCertPath,
                connectOptions);
        connection.setCallBack(new PubSubCallback());
        connection.connect();
        
        for (String topic : invalidTopics) {
            log.info("Topic length is {}", topic.getBytes().length);
            CheckCommon.checkIllegalTopicPermission(connection, topic, qos);
            connection.connect();
        }
    }
}
