package function;

import test.EDGEIntegrationTest;
import utils.Converter;
import utils.PubSubCommon;
import utils.client.ConnectionType;
import utils.client.EdgeDeviceMQTTChecker;
import utils.client.MqttConnection;
import utils.client.PubSubCallback;
import utils.client.RandomNameHolder;
import utils.fusesource.FuseConnectedAndSubCallback;
import utils.fusesource.FuseSubNoAckListener;
import utils.msg.EdgeDeviceNestMsg;
import utils.msg.EdgeDeviceSimpleMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for topic router of edge subscriptions.
 *
 * TestType: INTEGRATION_TEST
 *
 * IcafeId: bce-iot-4825
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceReTopicTest extends EDGEIntegrationTest {

    public ConnectionType connectionType;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions connectOptions;
    public static EdgeDeviceMQTTChecker checker;

    @Before
    public void setUp() throws Exception {
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL
                .equals(connectionType) ? true : false;
        qos = random.nextInt(2);
        log.info("Connection type is {}", connectionType.toString());
        connectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
        MqttConnection sampleCon = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("Sample_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
        checker = new EdgeDeviceMQTTChecker();
        checker.bindClient(sampleCon);
    }
    
    @After
    public void tearDown() throws Exception {
        checker.clean();
    }

    /**
     * TestGoal: Test msg forward of subscriptions which source topics containing wildcard characters(#/+)
     *
     * @throws Exception
     */
    @Test
    public void testTopicWildCard() throws Exception {
        // Source topic is wildcard/sharp/#, plus/wildcard/+
        qos = 0; // For order test
        String sharpTargetTopic = "result/wildcard/sharp";
        String plusTargetTopic = "result/wildcard/plus";
        Map<String, List<String>> correctTopics = new HashMap<String, List<String>>();
        correctTopics.put(sharpTargetTopic, Arrays.asList("wildcard/sharp/A/B", "wildcard/sharp/A", "wildcard/sharp"));
        correctTopics.put(plusTargetTopic, Arrays.asList("plus/wildcard/A", "plus/wildcard/B", "plus/wildcard/"));
        
        Map<String, List<String>> wrongTopics = new HashMap<String, List<String>>();
        wrongTopics.put(sharpTargetTopic, Collections.singletonList("wildcardsharp"));
        wrongTopics.put(plusTargetTopic, Arrays.asList("plus/wildcard/A/B", "plus/wildcard"));

        checker.bindTopic(sharpTargetTopic);
        checker.bindTopic(plusTargetTopic);
        checker.startSub();
        
        String pubClientId = "wildcard" + System.currentTimeMillis();
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, connectOptions);
        
        Map<String, List<String>> expectedMsgs = new HashMap<String, List<String>>();
        for (String targetTopic : correctTopics.keySet()) {
            for (String topic : correctTopics.get(targetTopic)) {
                EdgeDeviceSimpleMsg innerMsg = new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System
                        .currentTimeMillis());
                EdgeDeviceNestMsg nestMsg = new EdgeDeviceNestMsg(RandomNameHolder.getRandomString(5), "" + System
                        .currentTimeMillis(), innerMsg);
                String msgStr = Converter.modelToJsonUsingJsonNode(nestMsg);
                PubSubCommon.publish(pub, topic, qos, msgStr, false);

                if (expectedMsgs.containsKey(targetTopic)) {
                    expectedMsgs.get(targetTopic).add(msgStr);
                } else {
                    expectedMsgs.put(targetTopic, new ArrayList<String>(Collections.singletonList(msgStr)));
                }
            }
        }
        
        for (String targetTopic : wrongTopics.keySet()) {
            for (String topic : wrongTopics.get(targetTopic)) {
                EdgeDeviceSimpleMsg innerMsg = new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System
                        .currentTimeMillis());
                EdgeDeviceNestMsg nestMsg = new EdgeDeviceNestMsg(RandomNameHolder.getRandomString(5), "" + System
                        .currentTimeMillis(), innerMsg);
                String msgStr = Converter.modelToJsonUsingJsonNode(nestMsg);
                PubSubCommon.publish(pub, topic, qos, msgStr, false);
                Thread.sleep(200);
            }
        }
        Thread.sleep(SLEEP_TIME);
        
         checker.checkResultMessage(sharpTargetTopic, expectedMsgs.get(sharpTargetTopic));
        checker.checkResultMessage(plusTargetTopic, expectedMsgs.get(plusTargetTopic));
        pub.disconnect();
    }

    /**
     * TestGoal: Test message reforward feature in rules.
     *
     * IcafeId: bce-iot-5676
     *
     * Main steps:
     *  Step1: Connect one sub. Make callback do not send puback
     *  Step2: Connect a pub and publish a msg. Check sub will receive this msg immediately.
     *  Step3: Wait for time of reforward interval and check sub will receive
     *   the same msg again from target topic but not source topic
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testMessageReforwardInRule() throws Exception {
        String sourceTopic = "reforward/source";
        String targetTopic = "reforward/target";
        connectionType = random.nextBoolean() ? ConnectionType.SSL : ConnectionType.TCP;
        tls = ConnectionType.SSL.equals(connectionType) ? true : false;
        qos = 1;
        log.info("Connection type has changed to {}", connectionType.toString());
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        MQTT mqtt = PubSubCommon.initMqtt(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                offlineEdgePortMap), "wrongSub" + System.currentTimeMillis(), tls,
                edgeCertPath, connectOptions.getUserName(), new String(connectOptions.getPassword()));
        CallbackConnection wrongSub = mqtt.callbackConnection();
        FuseSubNoAckListener wrongSubListener = new FuseSubNoAckListener(mqtt);
        wrongSub.listener(wrongSubListener);
        FuseConnectedAndSubCallback callback = new FuseConnectedAndSubCallback(mqtt.getClientId().toString(),
                wrongSub, targetTopic, QoS.values()[qos]);
        wrongSub.connect(callback);
        // Wait for sub ready
        Assert.assertTrue("Sub is not ready", callback.waitForSubReady());

        try {
            // Pub a msg
            pub.connect();
            String msg = "reforward" + System.currentTimeMillis();
            PubSubCommon.publish(pub, sourceTopic, qos, msg, false);
            Thread.sleep(200);
            Assert.assertEquals("Missing msgs", Collections.singletonList(msg), wrongSubListener
                    .waitAndGetReveiveListMap(targetTopic, 1));
            wrongSubListener.setSendAck(true);
            wrongSubListener.getRawReceivedBufferMsgList(); // Clear

            Thread.sleep(FuseSubNoAckListener.REFORWARD_TIME);
            Assert.assertEquals("Missing msgs", Collections.singletonList(msg), wrongSubListener
                    .waitAndGetReveiveListMap(targetTopic, 1));
            List<Buffer> rawMsgList = wrongSubListener.getRawReceivedBufferMsgList();
            for (Buffer rawMsg : rawMsgList) {
                FuseSubNoAckListener.checkDupFlag(rawMsg, true);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            wrongSubListener.setSendAck(true);
            PubSubCommon.disconnectCallbackConnection(wrongSub);
            pub.disconnect();
            Thread.sleep(SLEEP_TIME);   // For ack
        }
    }

    /**
     * TestGoal: Test qos migrate in subscriptions topic router.
     *
     * IcafeId: bce-iot-5968
     *
     * @throws Exception
     */
    @Test
    public void testFunctionQosMigrate() throws Exception {
//    - source:
//        topic: 'qos/at_most_once'
//        qos: 0
//      target:
//        topic: 'qos/result/at_least_once'
//        qos: 1
//    - source:
//        topic: 'qos/at_most_once_function'
//        qos: 0
//        target:
//        type: 'function'
//        name: 'simple_sql'
//        topic: 'qos/result/function/at_least_once'
//        qos: 1
//    - source:
//        topic: 'qos/at_least_once'
//        qos: 1
//      target:
//        topic: 'qos/result/at_most_once'
//        qos: 0
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("qoscheck_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);

        try {
            connection.setCallBack(new PubSubCallback());
            connection.connect();

            List<String> sourceTopics = Arrays.asList("qos/at_most_once", "qos/at_most_once_function",
                    "qos/at_least_once");
            List<String> targetTopics = Arrays.asList("qos/result/at_least_once", "qos/result/function/at_least_once",
                    "qos/result/at_most_once");
            List<Integer> sourceQosList = Arrays.asList(0, 0, 1);
            List<Integer> targetQosList = Arrays.asList(1, 1, 0);
            for (Integer sourceQos : sourceQosList) {
                int index = sourceQosList.indexOf(sourceQos);
                String targetTopic = targetTopics.get(index);
                int targetQos = targetQosList.get(index);
                PubSubCommon.subscribe(connection, targetTopic, targetQos);

                String sourceTopic = sourceTopics.get(index);
                PubSubCommon.publish(connection, sourceTopic, sourceQos, "test", false);
                // Check msg qos will be min(pub qos, sub qos)
                log.info("Source qos is {} and target qos is {}", sourceQos, targetQos);
                connection.getCallback().waitAndGetReveiveList(1);
                int msgQos = connection.getCallback().getRawMqttMsgReceiveList().get(0).getQos();
                Assert.assertEquals("Qos migrate failed", Math.min(sourceQos, targetQos), msgQos);
            }

//        - source:
//            topic: 'qos/test/at_least_once'
//            qos: 1
//          target:
//            topic: 'qos/test/result/at_least_once'
//            qos: 1
            String sourceTopic = "qos/test/at_least_once";
            String targetTopic = "qos/test/result/at_least_once";
            int expectedQos = 1;
            PubSubCommon.subscribe(connection, targetTopic, expectedQos);
            PubSubCommon.publish(connection, sourceTopic, expectedQos, "test", false);
            connection.getCallback().waitAndGetReveiveList(1);
            int msgQos = connection.getCallback().getRawMqttMsgReceiveList().get(0).getQos();
            Assert.assertEquals("Qos migrate failed", expectedQos, msgQos);

            // Pub a msg with qos=0 and check target topic will receive msg of qos=0
            expectedQos = 0;
            PubSubCommon.publish(connection, sourceTopic, expectedQos, "test", false);
            connection.getCallback().waitAndGetReveiveList(1);
            msgQos = connection.getCallback().getRawMqttMsgReceiveList().get(0).getQos();
            Assert.assertEquals("Qos migrate failed", expectedQos, msgQos);
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }
}
