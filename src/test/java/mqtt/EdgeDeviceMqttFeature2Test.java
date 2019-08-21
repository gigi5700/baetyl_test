package mqtt;

import test.EDGEIntegrationTest;
import utils.CheckCommon;
import utils.PubSubCommon;
import utils.client.ConnectionType;
import utils.client.MqttConnection;
import utils.client.PubSubCallback;
import utils.fusesource.FuseConnectedAndSubCallback;
import utils.fusesource.FuseSubNoAckListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for mqtt features of mqtt device(For saving time in parallel running scenario)
 *
 * TestType: INTEGRATION_TEST
 *
 * IcafeId: bce-iot-4825
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceMqttFeature2Test extends EDGEIntegrationTest {
    
    public ConnectionType connectionType;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions connectOptions;
    
    @Before
    public void setUp() throws Exception {    
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL
                .equals(connectionType) ? true : false;
        qos = 1;
        log.info("Connection type is {}", connectionType.toString());
        connectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
    }
    
    /**
     * TestGoal: Test message reforward feature.
     *
     * Main steps:
     *  Step1: Connect two subs with qos = 0 and 1. Make callback do not send puback. Connect one normal sub.
     *  Step2: Connect a pub and publish two msgs. Check all subs will receive both msgs immediately.
     *  Step3: Wait for time of two reforward intervals and check sub of qos=1 will receive
     *   the same msgs again but sub of qos=0 and normal sub will not.
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testMessageReforward() throws Exception {
        String topic = "reforward";
        connectionType = random.nextBoolean() ? ConnectionType.SSL : ConnectionType.TCP;
        tls = ConnectionType.SSL.equals(connectionType) ? true : false;
        log.info("Connection type has changed to {}", connectionType.toString());
        MQTT mqtt = PubSubCommon.initMqtt(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                offlineEdgePortMap), "wrongSub" + System.currentTimeMillis(), tls,
                edgeCertPath, connectOptions.getUserName(), new String(connectOptions.getPassword()));
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        MqttConnection normalSub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("normalSub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        normalSub.setCallBack(new PubSubCallback());
        normalSub.connect();
        PubSubCommon.subscribe(normalSub, topic, qos);
        
        List<CallbackConnection> subs = new ArrayList<CallbackConnection>();
        List<FuseSubNoAckListener> listeners = new ArrayList<FuseSubNoAckListener>();
        try {
            for (int i = 0; i < 2; i++) {
                String clientId = String.format("qos%s_%s", i, System.currentTimeMillis());
                mqtt.setClientId(clientId);
                CallbackConnection wrongSub = mqtt.callbackConnection();
                FuseSubNoAckListener wrongSubListener = new FuseSubNoAckListener(mqtt);
                wrongSub.listener(wrongSubListener);
                listeners.add(wrongSubListener);
                FuseConnectedAndSubCallback callback = new FuseConnectedAndSubCallback(mqtt.getClientId().toString(),
                        wrongSub, topic, QoS.values()[i]);
                wrongSub.connect(callback);
                // Wait for sub ready
                Assert.assertTrue("Sub is not ready", callback.waitForSubReady()); 
            }
           
            // Send two msgs and check clients will receive immediately
            pub.connect();
            String msg = "reforward" + System.currentTimeMillis();
            PubSubCommon.publish(pub, topic, qos, msg, false);
            Thread.sleep(200);
            // Pub another msg after no puback sending to server
            String anotherMsg = "another" + System.currentTimeMillis();
            PubSubCommon.publish(pub, topic, qos, anotherMsg, false);
            Assert.assertEquals("Msgs for normal sub are wrong", Arrays.asList(msg, anotherMsg), normalSub
                    .getCallback().waitAndGetReveiveListMap(topic, 2, 3));
            for (FuseSubNoAckListener listener : listeners) {
                Assert.assertEquals("Missing msgs", Arrays.asList(msg, anotherMsg), listener
                            .waitAndGetReveiveListMap(topic, 2));
                listener.setSendAck(true);
                // Check dup flag
                List<Buffer> rawMsgList = listener.getRawReceivedBufferMsgList();
                for (Buffer rawMsg : rawMsgList) {
                    FuseSubNoAckListener.checkDupFlag(rawMsg, false);
                }
            }
            
            // Check reforward messages according to qos
            Thread.sleep(FuseSubNoAckListener.REFORWARD_TIME * 2 + 5000);
            for (FuseSubNoAckListener listener : listeners) {
                int index = listeners.indexOf(listener);
                if (index == 0) {
                    Assert.assertEquals("Receive unexpected msgs for qos 0", 0, listener
                            .waitAndGetReveiveListMap(topic, 1, 2).size());
                } else {
                    Assert.assertEquals("Missing msgs", Arrays.asList(msg, anotherMsg), listener
                            .waitAndGetReveiveListMap(topic, 2));
                    // Check dup flag
                    List<Buffer> rawMsgList = listener.getRawReceivedBufferMsgList();
                    for (Buffer rawMsg : rawMsgList) {
                        FuseSubNoAckListener.checkDupFlag(rawMsg, true);
                    }
                }
            }
            Assert.assertEquals("Receive unexpected msgs for normal sub", 0, normalSub
                    .getCallback().getReceiveListMap(topic).size());
        } catch (Exception e) {
            throw e;
        } finally {
            for (FuseSubNoAckListener listener : listeners) {
                listener.setSendAck(true);
            }
            
            for (CallbackConnection sub : subs) {
                PubSubCommon.disconnectCallbackConnection(sub);
            }
            
            pub.disconnect();
            Thread.sleep(SLEEP_TIME);   // For ack
        }
    }
    
    @Test
    public void testMsgBufferOfQos1() throws Exception {
        int qos1BufferSize = 5; // TODO move to conf
        qos = 1;
        connectionType = random.nextBoolean() ? ConnectionType.SSL : ConnectionType.TCP;
        tls = ConnectionType.SSL.equals(connectionType) ? true : false;
        log.info("Connection type has changed to {}", connectionType.toString());
        String topic = "qos1/buffer/check" + System.currentTimeMillis();
        
        MQTT mqtt = PubSubCommon.initMqtt(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                offlineEdgePortMap), "wrongSub" + System.currentTimeMillis(), tls,
                edgeCertPath, connectOptions.getUserName(), new String(connectOptions.getPassword()));
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        // Connect a client and set sendAck to false
        CallbackConnection wrongSub = mqtt.callbackConnection();
        FuseSubNoAckListener wrongSubListener = new FuseSubNoAckListener(mqtt);
        wrongSub.listener(wrongSubListener);
        FuseConnectedAndSubCallback callback = new FuseConnectedAndSubCallback(mqtt.getClientId().toString(),
                wrongSub, topic, QoS.values()[qos]);
        wrongSub.connect(callback);
        Assert.assertTrue("Sub is not ready", callback.waitForSubReady()); 
        
        try {
            // Publish msgs more than buffer, sub will receive first buffer size msg + 2
            // + 2 is because first one is picked out for ack(not in buffer), last one has been sent and wait for
            // added to buffer channel(Msg will be sent before added to channel)
            int expectCount = qos1BufferSize + 2;
            int msgCount = expectCount + 1 + random.nextInt(3);
            List<String> noAckMsgs = PubSubCommon.publishMessage(pub, topic, 0, msgCount, qos, false);
            List<String> bufferedMsgs = noAckMsgs.subList(0, expectCount);
            Assert.assertEquals("Received msgs are wrong", bufferedMsgs, wrongSubListener
                    .waitAndGetReveiveListMap(topic, noAckMsgs.size(), 5)); // Wait for more time to check no more msgs
                                                                            // will be received
            // Set sendAck to true
            wrongSubListener.setSendAck(true);

            // Wait for all msgs
            Thread.sleep(FuseSubNoAckListener.REFORWARD_TIME * expectCount);
            List<String> resultMsgs = new ArrayList<String>();
            for (int index = 0; index < bufferedMsgs.size(); index++) {
                resultMsgs.add(bufferedMsgs.get(index));
                if (index + expectCount < noAckMsgs.size()) {
                    resultMsgs.add(noAckMsgs.get(index + expectCount));
                }
            }
            Assert.assertEquals("Received msgs are wrong", resultMsgs, wrongSubListener
                    .waitAndGetReveiveListMap(topic, resultMsgs.size(), 5));
        } catch (Exception e) {
            throw e;
        } finally {
            wrongSubListener.setSendAck(true);
            pub.disconnect();
            PubSubCommon.disconnectCallbackConnection(wrongSub);
        }
    }
    
    @Test
    public void testMsgBufferOfQos0() throws Exception {
        int qos0BufferSize = 10;
        qos = 0;
        connectionType = random.nextBoolean() ? ConnectionType.SSL : ConnectionType.TCP;
        tls = ConnectionType.SSL.equals(connectionType) ? true : false;
        log.info("Connection type has changed to {}", connectionType.toString());
        String topic = "qos0/buffer/check" + System.currentTimeMillis();
        
        MQTT mqtt = PubSubCommon.initMqtt(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                offlineEdgePortMap), "wrongSub" + System.currentTimeMillis(), tls,
                edgeCertPath, connectOptions.getUserName(), new String(connectOptions.getPassword()));
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        // Connect a client and set sendAck to false
        CallbackConnection wrongSub = mqtt.callbackConnection();
        FuseSubNoAckListener wrongSubListener = new FuseSubNoAckListener(mqtt);
        wrongSub.listener(wrongSubListener);
        FuseConnectedAndSubCallback callback = new FuseConnectedAndSubCallback(mqtt.getClientId().toString(),
                wrongSub, topic, QoS.values()[qos]);
        wrongSub.connect(callback);
        Assert.assertTrue("Sub is not ready", callback.waitForSubReady()); 
        
        // Publish msgs more than buffer
        // Do not send ack for first msg
        List<String> noAckMsgs = PubSubCommon.publishMessage(pub, topic, 0, 1, qos, false);
        Thread.sleep(1000);
        wrongSubListener.setSendAck(true);
        int msgCount = qos0BufferSize + 5 + random.nextInt(5);
        List<String> normalMsgs = PubSubCommon.publishMessage(pub, topic, 1, msgCount, qos, false);
        
        Thread.sleep(FuseSubNoAckListener.REFORWARD_TIME + SLEEP_TIME);
        // Check client can only received first buffer size msgs
        List<String> expectedMsgs = new ArrayList<>();
        expectedMsgs.addAll(noAckMsgs);
        expectedMsgs.addAll(normalMsgs);
        Assert.assertEquals("Received msgs are wrong", expectedMsgs, wrongSubListener.waitAndGetReveiveListMap(topic,
                msgCount + 1, 5)); // Wait for more time to check no more msgs will be received
    }
    
    @Test
    public void testPersistentMsgCleanUp() throws Exception {
        // TODO 2min
        int cleanUp = 120;
        int interval = 10;
        qos = 1;
        String topic = "clean/up" + System.currentTimeMillis();
        // Connect 2 subs with clean session=false, subscribe topic and disconnect
        MqttConnectOptions conOpts = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
        conOpts.setCleanSession(false);
        MqttConnection subBeforeCleanUp = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("before_clean_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOpts);
        subBeforeCleanUp.setCallBack(new PubSubCallback());
        MqttConnection subAfterCleanUp = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("after_clean_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOpts);
        subAfterCleanUp.setCallBack(new PubSubCallback());
        PubSubCommon.subscribe(subBeforeCleanUp, topic, qos);
        PubSubCommon.subscribe(subAfterCleanUp, topic, qos);
        Thread.sleep(1000);
        subBeforeCleanUp.disconnect();
        subAfterCleanUp.disconnect();
        
        // Pub a few msgs to this topic
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        List<String> msgs = PubSubCommon.publishMessage(pub, topic, 0, 5, qos, false);
        
        // Connect one sub before cleanup and check it will received correct msgs
        long now = System.currentTimeMillis();
        long sleepTime = cleanUp / 2 + random.nextInt(cleanUp / 2) - 10;
        log.info("Sleep for {}s and connect sub before cleanUp", sleepTime);
        Thread.sleep(sleepTime * 1000);
        subBeforeCleanUp.connect();
        PubSubCommon.checkPubAndSubResult(msgs, subBeforeCleanUp.getCallback().waitAndGetReveiveList(msgs.size()),
                qos);
        
        // Connect another one after cleanup and check it will received nothing
        sleepTime = now + (cleanUp + interval) * 1000 - System.currentTimeMillis() + SLEEP_TIME;
        log.info("Sleep for {}ms and connect sub after cleanUp", sleepTime);
        Thread.sleep(sleepTime);
        subAfterCleanUp.connect();
        PubSubCommon.checkPubAndSubResult(new ArrayList<String>(), subBeforeCleanUp.getCallback()
                .waitAndGetReveiveList(msgs.size(), 3), qos);
    }
    
    /**
     * TestGoal: Test qos validation feature. Subscribing with illegal qos will fail. Connection publishing with
     *  illegal qos will be kicked out.
     *
     * @throws Exception
     */
    @Test
    public void testQosCheck() throws Exception {
        String topic = "qosCheck";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("qoscheck_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("qoscheckPub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        
        try {
            connection.setCallBack(new PubSubCallback());
            connection.connect();
            pub.connect();

            List<Integer> qosList = Arrays.asList(0, 1);
            for (Integer qos : qosList) {
                int code = PubSubCommon.subscribeWithReturn(connection, topic, qos).getGrantedQos()[0];
                Assert.assertEquals("Reason code is wrong", qos.intValue(), code);
                PubSubCommon.unsubscribe(connection, topic);
            }
            
            // Sub qos not 0,1 TODO other qos will be checked by paho lib
            List<Integer> illegalQosList = Arrays.asList(2);
            for (Integer qos : illegalQosList) {
                int code = PubSubCommon.subscribeWithReturn(connection, topic, qos).getGrantedQos()[0];
                Assert.assertEquals("Reason code is wrong", MqttException.REASON_CODE_SUBSCRIBE_FAILED, code);
                PubSubCommon.subscribe(connection, topic, 1);
                PubSubCommon.publish(pub, topic, qos, "test", random.nextBoolean());
                Thread.sleep(1000);
                Assert.assertFalse("Connection should be lost", pub.getClient().isConnected());

                // Nothing received
                Assert.assertEquals("Receive unexpected msg", 0, connection.getCallback().getReceiveList().size());
            }

            // Last will qos check
            MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                    offlineEdgePassword);
            conOptions.setWill(topic, "willmsg".getBytes(), 2, false);
            pub.disconnect();
            pub.setConnOpts(conOptions);
            CheckCommon.checkConnectionFail(pub);
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
            pub.disconnect();
        }
    }

    /**
     * TestGoal: Test qos migrate in topic router process.
     *
     * IcafeId: bce-iot-5968
     *
     * @throws Exception
     */
    @Test
    public void testQosMigrate() throws Exception {
        String topic = "qosMigrate";
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("qoscheck_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("qoscheckPub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);

        try {
            connection.setCallBack(new PubSubCallback());
            connection.connect();
            pub.connect();

            List<Integer> subQosList = Arrays.asList(0, 1);
            List<Integer> pubQosList = Arrays.asList(1, 0);
            for (Integer subQos : subQosList) {
                PubSubCommon.subscribe(connection, topic, subQos);

                int pubQos = pubQosList.get(subQosList.indexOf(subQos));
                PubSubCommon.publish(pub, topic, pubQos, "test", false);
                // Check msg qos will be min(pub qos, sub qos)
                log.info("Pub qos is {} and sub qos is {}", pubQos, subQos);
                connection.getCallback().waitAndGetReveiveList(1);
                int msgQos = connection.getCallback().getRawMqttMsgReceiveList().get(0).getQos();
                Assert.assertEquals("Qos migrate failed", Math.min(subQos, pubQos), msgQos);

                PubSubCommon.unsubscribe(connection, topic);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
            pub.disconnect();
        }
    }
    
    /**
     * TestGoal: Test retain feature of one client.
     *
     * @throws Exception
     */
    @Test
    public void testMqttFeatureBySelf() throws Exception {
        String topic = "selfCheck" + System.currentTimeMillis();
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("selfcheck_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        try {
            connection.setCallBack(new PubSubCallback());
            connection.connect();
            List<String> pubMsgs = PubSubCommon.publishMessage(connection, topic, 0, 5, qos, true);
            connection.disconnect();
            Thread.sleep(SLEEP_TIME);

            connection.connect();
            PubSubCommon.subscribe(connection, topic, qos);
            PubSubCommon.checkRetainResult(pubMsgs, connection.getCallback().waitAndGetReveiveList(), qos, true);
            
            PubSubCommon.unsubscribe(connection, topic);
            Thread.sleep(SLEEP_TIME);
            PubSubCommon.publish(connection, topic, qos, "", true);
            Thread.sleep(2000);
            PubSubCommon.subscribe(connection, topic, qos);
            Assert.assertEquals("Receive unexpected msg", 0, connection.getCallback().waitAndGetReveiveList(1, 3)
                    .size());
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }

    /**
     * TestGoal: Test unsubscribe feature.
     *
     * @throws Exception
     */
    @Test
    public void testNormalUnsub() throws Exception {
        String topic = "normal/unsub" + System.currentTimeMillis();
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("connection_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.subscribe(connection, topic, qos);
            PubSubCommon.publish(connection, topic, qos, topic, false);
            PubSubCommon.checkPubAndSubResult(Collections.singletonList(topic), connection.getCallback()
                    .waitAndGetReveiveListMap(topic, 1), qos);

            // Unsubscribe this topic
            PubSubCommon.unsubscribe(connection, topic);
            PubSubCommon.publish(connection, topic, qos, "unexpected" + System.currentTimeMillis(), false);
            Assert.assertEquals("Unexpected msgs after unsubscribe", 0, connection.getCallback()
                    .waitAndGetReveiveListMap(topic, 1, 3).size());
        } catch (Exception e) {
            throw e;
        } finally {
            PubSubCommon.unsubscribe(connection, topic);
            connection.disconnect();
        }
    }

    /**
     * TestGoal: Test subscribing a few topics containing illegal or unauthorized ones with normal or illegal qos,
     *  then check each subscribe will be handled seperately.
     *
     * @throws Exception
     */
    @Test
    public void testSubAfewTopics() throws Exception {
        String topic = "few/topics/check";
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("fewTopicsCheckPub_%s", System.currentTimeMillis()),
                tls, edgeCertPath, connectOptions);
        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(edgeSingleTopicUsername,
                edgeSingleTopicPassword);
        MqttConnection sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("fewTopicsCheckSub_%s", System.currentTimeMillis()),
                tls, edgeCertPath, conOptions);
        sub.setCallBack(new PubSubCallback());
        sub.connect();
        
        // Subscribe a few topics and check suback codes
        String[] topics = new String[]{topic, "noPermssion", "++illegalTopic", "illegalQos"};
        int[] qosList = new int[]{qos, qos, qos, 2};
        IMqttToken token = sub.getClient().subscribe(topics, qosList);
        token.waitForCompletion(MqttConnection.ACTION_TIME_OUT);
        int[] code = token.getGrantedQos();
        log.info("Code list is {}", Arrays.toString(code));
        for (int i = 0; i < code.length; i++) {
            if (i == 0) {  // Only first one will be successful
                Assert.assertEquals("Reason code is wrong", qos, code[i]);
            } else {
                Assert.assertEquals("Reason code is wrong", MqttException.REASON_CODE_SUBSCRIBE_FAILED, code[i]);
            }
        }
        
        for (int i = 0; i < topics.length; i++) {
            PubSubCommon.publish(pub, topics[i], qos, topics[i], false);
            Thread.sleep(200);
        }
        Thread.sleep(SLEEP_TIME);
        List<String> receivedMsgs = sub.getCallback().getReceiveList();
        Assert.assertEquals("Receive unexpected msg", 1, receivedMsgs.size());
        Assert.assertEquals("Msg contents are wrong", topic, receivedMsgs.get(0));
        sub.disconnect();
        pub.disconnect();
    }
}
