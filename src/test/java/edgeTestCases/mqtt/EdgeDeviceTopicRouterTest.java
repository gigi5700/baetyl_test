package edgeTestCases.mqtt;

import edgeTestCases.EDGEIntegrationTest;
import edgeTestCases.utils.PubSubCommon;
import edgeTestCases.utils.client.ConnectionType;
import edgeTestCases.utils.client.MqttConnection;
import edgeTestCases.utils.client.PubSubCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for topic router of edgeTestCases.mqtt device
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceTopicRouterTest extends EDGEIntegrationTest {

    public ConnectionType connectionType;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions connectOptions;
    public static final String CHARSET = "utf-8";

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
     * TestGoal: Test topic filter of wildcard of edgeTestCases.mqtt topics.(#/+)
     *
     * @throws Exception
     */
    @Test
    public void testTopicFilter() throws Exception {
        List<String> subTopics = Arrays.asList("TEST/SUB/A/B/C", "TEST/SUB/A/#", "TEST/SUB/A/+", "TEST/+/A", "+/SUB/A",
                "+/SUB/#", "+/SUB/+", "+/+/+");
        List<String> pubTopics = Arrays.asList("TEST/SUB/A/B/C/D", "TEST/SUB/A/B/C", "TEST/SUB/A/B", "TEST/SUB/A",
                "TEST/SUB/A/", "OTHER/SUB/A", "OTHER/SUB/A/B/C");
        List<String> topics = new ArrayList<String>();
        topics.addAll(pubTopics);
        topics.addAll(subTopics);

        List<MqttConnection> subList = new ArrayList<MqttConnection>();
        MqttConnection pub = null;
        try {
            // Start subs
            for (String topic : subTopics) {
                MqttConnection sub = PubSubCommon.createMqttConnection(
                        PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                        String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
                sub.setCallBack(new PubSubCallback());
                sub.connect();
                PubSubCommon.subscribe(sub, topic, qos);
                subList.add(sub);
            }

            // Pub messages
            pub = PubSubCommon.createMqttConnection(
                    PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                    String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
            pub.connect();
            List<List<Integer>> receiveSubIndexList = Arrays.asList(
                    Arrays.asList(1, 5), 
                    Arrays.asList(0, 1, 5),
                    Arrays.asList(1, 2, 5),
                    Arrays.asList(1, 3, 4, 5, 6, 7), 
                    Arrays.asList(1, 2, 5), 
                    Arrays.asList(4, 5, 6, 7),
                    Arrays.asList(5));

            for (String topic : pubTopics) {
                int messages = random.nextInt(5) + 5;
                List<String> pubMessages = PubSubCommon.publishMessage(pub, topic, 0, messages, qos, false);

                // Check subs should receive message
                List<Integer> subIndexList = receiveSubIndexList.get(pubTopics.indexOf(topic));
                for (Integer index : subIndexList) {
                    log.info("Check topic {}, index {}", topic, index);
                    PubSubCommon.checkPubAndSubResult(pubMessages, subList.get(index).getCallback()
                            .waitAndGetReveiveList(pubMessages.size()), qos);
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            // Disconnect sub and pub
            if (pub != null) {
                pub.disconnect();
            }
            for (MqttConnection sub : subList) {
                sub.disconnect();
            }
        }
    }
    
    /**
     * TestGoal: Test msgs can forward between different principals.
     *
     * Main steps:
     *  Step1: Connect two clients of different principals, then publish msgs to same topic.
     *  Step2: Check each sub will receive the total msg.
     *
     * @throws Exception
     */
    @Test
    public void testPubAndSubWithOneTopicButDifferentPrincipals() throws Exception {
        String topic = "CommonTopic";
        MqttConnection connectionA = null;
        MqttConnection connectionB = null;

        try {
            connectionA = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                    offlineEdgePortMap), String.format("ConnectionA_%s", System.currentTimeMillis()), tls, edgeCertPath,
                    connectOptions);
            connectionA.setCallBack(new PubSubCallback());
            connectionA.connect();
            PubSubCommon.subscribe(connectionA, topic, qos);
            
            MqttConnectOptions connectOptionsB = PubSubCommon.getDefaultConnectOptions(offlineEdgeAnotherUsername,
                    offlineEdgeAnotherPassword);
            connectionB = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                    offlineEdgePortMap), String.format("ConnectionB_%s", System.currentTimeMillis()), tls,
                    edgeCertPath, connectOptionsB);
            connectionB.setCallBack(new PubSubCallback());
            connectionB.connect();
            PubSubCommon.subscribe(connectionB, topic, qos);

            List<String> pubMessages = PubSubCommon.publishMessage(connectionA, topic, 0, 5, qos, false);
            pubMessages.addAll(PubSubCommon.publishMessage(connectionB, topic, 5, 10, qos, false));
            PubSubCommon.checkPubAndSubResult(pubMessages, connectionA.getCallback().waitAndGetReveiveList(pubMessages
                    .size()), qos);
            PubSubCommon.checkPubAndSubResult(pubMessages, connectionB.getCallback().waitAndGetReveiveList(pubMessages
                    .size()), qos);
        } catch (Exception e) {
            throw e;
        } finally {
            if (connectionA != null) {
                PubSubCommon.unsubscribe(connectionA, topic);
                connectionA.disconnect();
            }

            if (connectionB != null) {
                PubSubCommon.unsubscribe(connectionB, topic);
                connectionB.disconnect();
            }
        }
    }
    
    /**
     * TestGoal: Test msg receive event of topic containing Chinese character.
     *
     * @throws Exception
     */
    @Test
    public void testChineseTopic() throws Exception {
        String topic = new String("test/Chinese/中文".getBytes(CHARSET), CHARSET);
        List<String> subTopics = Arrays.asList(topic, "#", "test/Chinese/+", "test/Chinese/#", topic);
        MqttConnection pub = null;
        
        List<MqttConnection> subList = new ArrayList<MqttConnection>();
        try {
            for (String subTopic : subTopics) {
                MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                        offlineEdgeUrl, ConnectionType.TCP, offlineEdgePortMap),
                        String.format("chinese_%s", System.currentTimeMillis()), false, edgeCertPath,
                        connectOptions);
                connection.setCallBack(new PubSubCallback());
                subList.add(connection);
                connection.connect();
                PubSubCommon.subscribe(connection, new String(subTopic.getBytes(CHARSET), CHARSET), qos);
            }
            Thread.sleep(SLEEP_TIME);
            for (MqttConnection sub : subList) {  // Clear dirty msgs like retain msg
                sub.getCallback().clear();
            }

            // Pub connects and publishes a few messages
            int pubMsgSize = 5 + random.nextInt(5);
            log.info("Pub {} msg to chinese topic", pubMsgSize);
            pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                    offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath,
                    connectOptions);
            pub.connect();
            List<String> pubMessages = PubSubCommon.publishChineseMessage(pub, topic, 0, pubMsgSize, qos, false);

            // Check sub result
            for (MqttConnection sub : subList) {
                log.info("Checking topic {}", subTopics.get(subList.indexOf(sub)));
                PubSubCommon.checkPubAndSubResult(pubMessages, sub.getCallback().waitAndGetReveiveList(pubMessages
                        .size()), qos);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            for (MqttConnection sub : subList) {
                sub.disconnect();
            }

            if (pub != null) {
                pub.disconnect();
            }
        }
    }
    
    /**
     * TestGoal: Test unsub event of one topic for edgeTestCases.mqtt broker(Will not receive msgs of this topic anymore)
     *
     * @throws Exception
     */
    @Test
    public void testUnsub() throws Exception {
        String topic = "unsub";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.subscribe(connection, topic, qos);
            List<String> pubMsg = PubSubCommon.publishMessage(connection, topic, 0, 3, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsg, connection.getCallback().waitAndGetReveiveListMap(topic, pubMsg
                    .size()), qos);
            PubSubCommon.unsubscribe(connection, topic);
            pubMsg = PubSubCommon.publishMessage(connection, topic, 3, 5, qos, false);
            Assert.assertEquals("Receive unexpected msgs", 0, connection.getCallback().waitAndGetReveiveListMap(topic,
                    pubMsg.size(), 3).size());
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }
    
    /**
     * TestGoal: Test msg router feature of case sensitive topics.
     *
     * @throws Exception
     */
    @Test
    public void testCaseSensitiveTopic() throws Exception {
        String topic = "Case/Sensitive";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.subscribe(connection, topic, qos);
            PubSubCommon.subscribe(connection, topic.toUpperCase(), qos);
            PubSubCommon.subscribe(connection, topic.toLowerCase(), qos);

            List<String> pubMsg = PubSubCommon.publishMessage(connection, topic, 0, 3, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsg, connection.getCallback().waitAndGetReveiveListMap(topic, pubMsg
                    .size()), qos);
            Thread.sleep(1000);
            Assert.assertEquals("Receive unexpected msgs", 0, connection.getCallback().getReceiveListMap(topic
                    .toLowerCase()).size());
            Assert.assertEquals("Receive unexpected msgs", 0, connection.getCallback().getReceiveListMap(topic
                    .toUpperCase()).size());
        } catch (Exception e) {
            throw e;
        } finally {
            PubSubCommon.unsubscribe(connection, topic);
            PubSubCommon.unsubscribe(connection, topic.toUpperCase());
            PubSubCommon.unsubscribe(connection, topic.toLowerCase());
            connection.disconnect();
        }
    }

    /**
     * TestGoal: Test msg router feature of special topics.
     *
     * @throws Exception
     */
    @Test
    public void testSpecialTopicRouter() throws Exception {
        // TODO add more topics
        List<String> specialTopics = Arrays.asList(" blank", "blank ", "b lank", "a//b", " ");
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        connection.connect();
        PubSubCommon.subscribe(connection, specialTopics, qos);
        for (String topic : specialTopics) {
            List<String> pubMsgs = PubSubCommon.publishMessage(connection, topic, 0, 3, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsgs, connection.getCallback().waitAndGetReveiveList(pubMsgs.size()),
                    qos);
            PubSubCommon.checkPubAndSubResult(pubMsgs, connection.getCallback().waitAndGetReveiveListMap(topic,
                    pubMsgs.size()), qos);
        }
    }
}
