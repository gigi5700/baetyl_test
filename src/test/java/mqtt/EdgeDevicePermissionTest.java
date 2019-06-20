package mqtt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import test.EDGEIntegrationTest;
import util.client.ConnectionType;
import util.CheckCommon;
import util.client.MqttConnection;
import util.client.PubSubCallback;
import util.client.RandomNameHolder;
import util.PubSubCommon;

/**
 * Test cases for permission check of mqtt device
 *
 * TestType: INTEGRATION_TEST
 *
 * IcafeId: bce-iot-4825
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDevicePermissionTest extends EDGEIntegrationTest {

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
     * TestGoal: Test connecting with wrong credentials.
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithWrongCredentials() throws Exception {
        MqttConnectOptions connectOpts = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, "wrong");
        MqttConnection connection = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOpts);
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_FAILED_AUTHENTICATION);
        
        connectOpts = PubSubCommon.getDefaultConnectOptions("wrong", offlineEdgeAnotherPassword);
        connection.setConnOpts(connectOpts);
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_FAILED_AUTHENTICATION);

        connectOpts = PubSubCommon.getDefaultConnectOptions("wrong", "wrong");
        connection.setConnOpts(connectOpts);
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_FAILED_AUTHENTICATION);
    }
    
    /**
     * TestGoal: Test connection with unauthorized last will topic will be kicked out.
     *
     * @throws Exception
     */
    @Test
    public void testLastWillPermission() throws Exception {
        String topic = "lastwill_permission";
        String willMessage = "die";

        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(edgeSingleTopicUsername,
                edgeSingleTopicPassword);
        conOptions.setWill(topic, willMessage.getBytes(), qos, false);
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        CheckCommon.checkConnectionFail(pub, MqttException.REASON_CODE_NOT_AUTHORIZED);
    }
    
    /**
     * TestGoal: Test operation permission checker of mqtt broker.
     *
     * Main steps:
     *  Step1: Connect one connection A with PUB operation of topicA and SUB operation of topicB.
     *  Step2: Connect another connection B having all permissions of topics in Step1. Then both subscribe topics
     *   in Step1.
     *  Step3: Connection A publish a msg to topicA, then connection B will receive this msg but connection A not.
     *  Step4: Connection B publish a msg to topicB, check both connections can receive this msg.
     *  Step4: Connection A publish a msg to topicB and check connection will be kicked out.
     *
     * @throws Exception
     */
    @Test
    public void testOperationPermission() throws Exception {
        String pubTopic = "single/topic/pub";        // Topic with pub permission
        String subTopic = "single/topic/sub";        // Topic with sub permission
        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(edgeSingleTopicUsername,
                edgeSingleTopicPassword);
        MqttConnection singleCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Single_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        MqttConnection allCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("all_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        try {
            singleCon.setCallBack(new PubSubCallback());
            allCon.setCallBack(new PubSubCallback());
            singleCon.connect();
            allCon.connect();
            PubSubCommon.subscribe(singleCon, Arrays.asList(pubTopic, subTopic), qos);
            PubSubCommon.subscribe(allCon, Arrays.asList(pubTopic, subTopic), qos);

            // Publish with singlePub
            List<String> pubMsgs = PubSubCommon.publishMessage(singleCon, pubTopic, 0, 5, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsgs, allCon.getCallback().waitAndGetReveiveListMap(pubTopic,
                    pubMsgs.size()), qos);
            Assert.assertEquals("Receive unexpected msg", 0, singleCon.getCallback().getReceiveList().size());

            // Publish with all
            pubMsgs = PubSubCommon.publishMessage(allCon, subTopic, 0, 5, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsgs, allCon.getCallback().waitAndGetReveiveListMap(subTopic,
                    pubMsgs.size()), qos);
            PubSubCommon.checkPubAndSubResult(pubMsgs, singleCon.getCallback().waitAndGetReveiveListMap(subTopic,
                    pubMsgs.size()), qos);
            CheckCommon.checkDevicesHavingNoPubAuthority(Collections.singletonList(singleCon), subTopic);
        } catch (Exception e) {
            throw e;
        } finally {
            singleCon.disconnect();
            allCon.disconnect();
        }
    }
    
    /**
     * TestGoal: Test topic permission checker of mqtt broker(There should be fail flag in suback when subscribing
     *  a topic without permission and connection should be kicked out)
     *
     * @throws Exception
     */
    @Test
    public void testTopicPermission() throws Exception {
        String topic = "noPermission" + System.currentTimeMillis();
        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(edgeSingleTopicUsername,
                edgeSingleTopicPassword);
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        MqttConnection sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Sub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        sub.setCallBack(new PubSubCallback());
        sub.connect();
        int code = PubSubCommon.subscribeWithReturn(sub, topic, qos).getGrantedQos()[0];
        Assert.assertEquals("Reason code is wrong", MqttException.REASON_CODE_SUBSCRIBE_FAILED, code);
        CheckCommon.checkDevicesHavingNoPubAuthority(Collections.singletonList(pub), topic);
        Assert.assertEquals("Receive unexpected msg", 0, sub.getCallback().waitAndGetReveiveList(1, 3).size());
        sub.disconnect();
    }

    /**
     * TestGoal: Test permission checker with topics containing wildcard characters.
     *
     * @throws Exception
     */
    @Test
    public void testWildCardConfigTopicPermissionInMultiLayer() throws Exception {
        // One principal has pub and sub permissions of 'test/permit/plus/+' and 'test/permit/sharp/#'
        String sharpPermitTopic = "test/permit/sharp/#";
        String plusPermitTopic = "test/permit/plus/+";
        List<String> topicsForSharp = Arrays.asList("test/permit/sharp/a", "test/permit/sharp", "test/permit/sharp/",
                "test/permit/sharp/a/b");
        List<String> topicsForPlus = Arrays.asList("test/permit/plus/a", "test/permit/plus/", "test/permit/plus/b");

        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(permitCheckUsername, permitCheckPassword);
        MqttConnection connectionForSharp = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionType, offlineEdgePortMap), String.format("sharp_%s",
                        System.currentTimeMillis()), tls, edgeCertPath, conOptions);
        connectionForSharp.setCallBack(new PubSubCallback());
        connectionForSharp.connect();
        PubSubCommon.subscribe(connectionForSharp, topicsForSharp, qos);
        PubSubCommon.subscribe(connectionForSharp, sharpPermitTopic, qos);

        MqttConnection connectionForPlus = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionType, offlineEdgePortMap), String.format("plus_%s",
                        System.currentTimeMillis()), tls, edgeCertPath, conOptions);
        connectionForPlus.setCallBack(new PubSubCallback());
        connectionForPlus.connect();
        PubSubCommon.subscribe(connectionForPlus, topicsForPlus, qos);
        PubSubCommon.subscribe(connectionForPlus, plusPermitTopic, qos);

        for (String topic : topicsForSharp) {
            String msg = "topic" + System.currentTimeMillis();
            PubSubCommon.publish(connectionForSharp, topic, qos, msg, false);
            Assert.assertEquals("Received msgs are wrong", Collections.singletonList(msg), connectionForSharp
                    .getCallback().waitAndGetReveiveListMap(topic, 2, 3)); // Wait for 2 is for checking no more msgs
        }

        for (String topic : topicsForPlus) {
            String msg = "topic" + System.currentTimeMillis();
            PubSubCommon.publish(connectionForPlus, topic, qos, msg, false);
            Assert.assertEquals("Received msgs are wrong", Collections.singletonList(msg), connectionForPlus
                    .getCallback().waitAndGetReveiveListMap(topic, 2, 3)); // Wait for 2 is for checking no more msgs
        }

        // Check operations
        String unauthorizedPermit = "test/permit/other";
        CheckCommon.checkIllegalTopicPermission(connectionForPlus, unauthorizedPermit, qos);
        CheckCommon.checkIllegalTopicPermission(connectionForSharp, unauthorizedPermit, qos);
        CheckCommon.checkIllegalTopicPubPermission(connectionForSharp, sharpPermitTopic, qos);
        CheckCommon.checkIllegalTopicPubPermission(connectionForPlus, plusPermitTopic, qos);
    }
    
    /**
     * TestGoal: Test permission checker with wildcard topics.
     *
     * Main steps:
     *  Step1: Threre are two principals in config, one permit is "#" for both pub and sub, another is "+".
     *  Step2: Connect two clients, one subscribes topics for "#" test and the other subscribes topics for "+" test.
     *  Step3: Publish msgs to each valid topic and check received msgs.
     *  Step4: Check unathorized topics permissions for client containing "+" permission.
     *
     * @throws Exception
     */
    @Test
    public void testWildCardConfigTopicPermission() throws Exception {
        String sharpPermitTopic = "#";
        String plusPermitTopic = "+";
        List<String> topicsForSharp = Arrays.asList("test", "test/", "test/permit/sharp/a/b", "test/#", "test/+", "+");
        List<String> topicsForPlus = Arrays.asList("test", "a");

        MqttConnection connectionForSharp = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionType, offlineEdgePortMap), String.format("sharp_%s",
                        System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
        connectionForSharp.setCallBack(new PubSubCallback());
        connectionForSharp.connect();
        PubSubCommon.subscribe(connectionForSharp, topicsForSharp, qos);
        PubSubCommon.subscribe(connectionForSharp, sharpPermitTopic, qos);

        // Check sharp permissions
        for (String topic : topicsForSharp) {
            if (topic.contains("#") || topic.contains("+")) {
                continue;
            }
            String msg = "topic" + System.currentTimeMillis();
            PubSubCommon.publish(connectionForSharp, topic, qos, msg, false);
            Assert.assertEquals("Received msgs are wrong", Collections.singletonList(msg), connectionForSharp
                    .getCallback().waitAndGetReveiveListMap(topic, 2, 3)); // Wait for 2 is for checking no more msgs
        }
        connectionForSharp.disconnect();
        
        // Check plus permissions
        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(onlyPlusUsername, onlyPlusPassword);
        MqttConnection connectionForPlus = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionType, offlineEdgePortMap), String.format("plus_%s",
                        System.currentTimeMillis()), tls, edgeCertPath, conOptions);
        connectionForPlus.setCallBack(new PubSubCallback());
        connectionForPlus.connect();
        PubSubCommon.subscribe(connectionForPlus, topicsForPlus, qos);
        PubSubCommon.subscribeWithReturn(connectionForPlus, plusPermitTopic, qos);

        for (String topic : topicsForPlus) {
            String msg = "topic" + System.currentTimeMillis();
            PubSubCommon.publish(connectionForPlus, topic, qos, msg, false);
            Assert.assertEquals("Received msgs are wrong", Collections.singletonList(msg), connectionForPlus
                    .getCallback().waitAndGetReveiveListMap(topic, 2, 3)); // Wait for 2 is for checking no more msgs
        }

        // Check unauthorized permissions
        CheckCommon.checkIllegalTopicPubPermission(connectionForSharp, sharpPermitTopic, qos);
        CheckCommon.checkIllegalTopicPubPermission(connectionForPlus, plusPermitTopic, qos);

        List<String> unauthorizedPermits = Arrays.asList("test/permit/other", "test/", "/test");
        for (String permit : unauthorizedPermits) {
            CheckCommon.checkIllegalTopicPermission(connectionForPlus, permit, qos);
        }
    }
    
    /**
     * TestGoal: Test unsubscribing topics which are not subscribed or having no permissions of SUB operation.
     *
     * @throws Exception
     */
    @Test
    public void testIllegalUnsub() throws Exception {
        String topic = "illegalUsub";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.unsubscribe(connection, topic);
            PubSubCommon.unsubscribe(connection, "no-permission");
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }

    /**
     * TestGoal: Test duplicate subscribe one topic of one client.
     *
     * @throws Exception
     */
    @Test
    public void testDuplicateSub() throws Exception {
        String topic = "duplicate/sub";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.subscribe(connection, topic, qos);
            
            List<String> pubMsgsDuringSub = new ArrayList<String>();
            // Pub msg with another thread
            Thread pubThread = new Thread(new Runnable() {
                
                @Override
                public void run() {
                    try {
                        pubMsgsDuringSub.addAll(PubSubCommon.publishMessage(connection, topic, 0, 10, qos, false));
                    } catch (Exception e) {
                        log.error("Pub error", e);
                    }
                }
            });
            pubThread.start();
            Thread.sleep(10);

            List<Integer> qosList = Arrays.asList(qos, 0, 1, 0, qos);
            for (Integer qosInteger : qosList) {
                int grantedQos = PubSubCommon.subscribeWithReturn(connection, topic, qosInteger.intValue())
                        .getGrantedQos()[0];
                Assert.assertEquals("Code is wrong", qosInteger.intValue(), grantedQos);
                Thread.sleep(10);
            }
            pubThread.join();
            PubSubCommon.checkPubAndSubResult(pubMsgsDuringSub, connection.getCallback()
                    .waitAndGetReveiveListMap(topic, pubMsgsDuringSub.size()), qos);
            connection.getCallback().getRawMqttMsgReceiveList();  // Clear
            
            // Pub again and check qos should be the last one
            List<String> pubMsg = PubSubCommon.publishMessage(connection, topic, 10, 13, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsg, connection.getCallback()
                    .waitAndGetReveiveListMap(topic, pubMsg.size()), qos);
            List<MqttMessage> rawMsgs = connection.getCallback().getRawMqttMsgReceiveList();
            for (MqttMessage msg : rawMsgs) {
                Assert.assertEquals("Qos is wrong of msg " + new String(msg.getPayload()), qos, msg.getQos());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            PubSubCommon.unsubscribe(connection, topic);
            connection.disconnect();
        }
    }
    
    /**
     * TestGoal: Test subscribing overlap topics and check mqtt will forward msgs according to qos.
     *
     * @throws Exception
     */
    @Test
    public void testOverlapSub() throws Exception {
        List<String> subTopics = Arrays.asList("test/a", "test/+", "test/a/#", "test/a/b/#");
        List<Integer> qosList = Arrays.asList(1, 0, 0, 1);
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        qos = 1;
        
        List<String> pubTopics = Arrays.asList("test/a", "test/b", "test/a/b");
        // According to sub qos
        List<Integer> qosListOfReceivedMsgs = Arrays.asList(1, 0, 1);
        Map<String, String> retainMsgs = new HashMap<String, String>();
        List<List<String>> receivedRetainMsgTopicsForSubTopics = Arrays.asList(
                Collections.singletonList("test/a"),  // For topic test/a
                Arrays.asList("test/a", "test/b"),    // For topic test/+
                Arrays.asList("test/a", "test/a/b"),  // For topic test/a/#
                Arrays.asList("test/a/b"));           // For topic test/a/b/#
        try {
            connection.connect();
            for (int index = 0; index < subTopics.size(); index++) {
                PubSubCommon.subscribe(connection, subTopics.get(index), qosList.get(index));
                log.info("Sub topic {} with qos {}", subTopics.get(index), qosList.get(index));
            }
            Thread.sleep(1000); // Wait for subscribe finished
            
            for (String pubTopic : pubTopics) {
                int index = pubTopics.indexOf(pubTopic);
                String msg = RandomNameHolder.getRandomString(10);
                log.info("Pub msg to topic {} with qos {}", pubTopic, qos);
                PubSubCommon.publish(connection, pubTopic, qos, msg, true);
                retainMsgs.put(pubTopic, msg);
                
                Assert.assertEquals("Received wrong msgs", msg, connection.getCallback()
                        .waitAndGetReveiveListMap(pubTopic, 1, 3).get(0));
                List<MqttMessage> rawMsgs = connection.getCallback().getRawMqttMsgReceiveList();
                Assert.assertEquals("Qos of received msg is wrong for topic " + pubTopic, qosListOfReceivedMsgs
                        .get(index).intValue(), rawMsgs.get(0).getQos());
            }
            
            // Check retain
            connection.disconnect();
            Thread.sleep(1000);
            connection.connect();
            for (String subTopic : subTopics) {
                int index = subTopics.indexOf(subTopic);
                PubSubCommon.subscribe(connection, subTopic, qosList.get(index));
                List<String> retainMsgTopics = receivedRetainMsgTopicsForSubTopics.get(index);
                for (String retainMsgTopic : retainMsgTopics) {
                    Assert.assertEquals("Received wrong msgs", retainMsgs.get(retainMsgTopic), connection.getCallback()
                            .waitAndGetReveiveListMap(retainMsgTopic, 1, 3).get(0));
                }
                // Check qos
                List<MqttMessage> rawMsgs = connection.getCallback().getRawMqttMsgReceiveList();
                for (MqttMessage rawMsg : rawMsgs) {
                    Assert.assertEquals("Qos of retain msg is wrong", qosList.get(index).intValue(), rawMsg.getQos());
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            // Clear retain
            for (String pubTopic : pubTopics) {
                PubSubCommon.publish(connection, pubTopic, 1, "", true);
            }
            connection.disconnect();
        }
    }
}
