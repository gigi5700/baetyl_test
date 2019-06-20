package mqtt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import test.EDGEIntegrationTest;
import util.PubSubCommon;
import util.client.ConnectionType;
import util.client.MqttConnection;
import util.client.PubSubCallback;

/**
 * Test cases for mqtt features of mqtt device
 *
 * TestType: INTEGRATION_TEST
 *
 * IcafeId: bce-iot-4825
 *
 * @author Zhao Meng
 */
@Slf4j
@RunWith(Parameterized.class)
public class EdgeDeviceMqttFeatureTest extends EDGEIntegrationTest {
    
    public ConnectionType connectionType;
    public boolean tls;
    public boolean retained;
    public boolean cleanSession;
    public int qos;
    public static MqttConnectOptions connectOptions;

    public EdgeDeviceMqttFeatureTest(boolean retained, boolean cleanSession, int qos) {
        super();
        this.retained = retained;
        this.cleanSession = cleanSession;
        this.qos = qos;
    }
    
    @SuppressWarnings("rawtypes")
    @Parameters(name = "retain: [{0}], cleanSession: [{1}], qos: [{2}]")
    public static Collection provideData() {
        boolean[] retainList = {true, false};
        boolean[] cleanSessionList = {true, false};
        int[] qosList = {0, 1};
     
        int items = retainList.length * cleanSessionList.length * qosList.length;
        Object[][] data = new Object[items][3];

        for (int i = 0; i < retainList.length; i++) {
            for (int j = 0; j < cleanSessionList.length; j++) {
                for (int k = 0; k < qosList.length; k++) {
                    int item = i * items / retainList.length + j * qosList.length + k;
                    data[item][0] = retainList[i];
                    data[item][1] = cleanSessionList[j];
                    data[item][2] = qosList[k];
                }
            }
        }
        return Arrays.asList(data);
    }

    @Before
    public void setUp() throws Exception {    
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL
                .equals(connectionType) ? true : false;
        log.info("Connection type is {}", connectionType.toString());
        connectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
    }
    
    /**
     * TestGoal: Test clean session feature of mqtt broker(Parameterized).
     *
     * Main steps:
     *  Step1: Connect a few subs with clean session = true/false.
     *  Step2: Connect one pub and publish a few msgs. Check each sub will receive expected msgs.
     *  Step3: Disconnect subs without unsubscribing. Pub publish msgs again. Then connect subs again
     *   and check received msgs according to clean session value.
     *
     * @throws Exception
     */
    @Test
    public void testCleanSession() throws Exception {
        String topic = "/cleanSession_/";

        List<MqttConnection> subList = new ArrayList<MqttConnection>();
        MqttConnection pub = null;
        try {
            MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                    offlineEdgePassword);
            conOptions.setCleanSession(cleanSession);

            // Start subs
            for (int i = 0; i < random.nextInt(3) + 2; i++) {
                MqttConnection sub = PubSubCommon.createMqttConnection(
                        PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                        String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, conOptions);
                sub.setCallBack(new PubSubCallback());
                sub.connect();
                PubSubCommon.subscribe(sub, topic, qos);
                subList.add(sub);
            }

            pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                    offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath,
                    conOptions);
            pub.connect();

            // Pub publishes 5 message and check sub messages
            List<String> pubMessages = PubSubCommon.publishMessage(pub, topic, 0, 5, qos, retained);
            for (MqttConnection sub : subList) {
                log.info("Check client {} with broker url {}", sub.getClient().getClientId(), sub.getClient()
                        .getServerURI());
                PubSubCommon.checkPubAndSubResult(pubMessages, sub.getCallback().waitAndGetReveiveList(pubMessages
                        .size()), qos);
                // Check retain flag should be false in normal msgs
                for (MqttMessage rawMsg : sub.getCallback().getRawMqttMsgReceiveList()) {
                    Assert.assertEquals("Retain flag is wrong", false, rawMsg.isRetained());
                }
            }
            
            // Subs disconnect and pub publishes 5 message
            for (MqttConnection sub : subList) {
                sub.disconnect();
            }
            
            List<String> saveMessages = PubSubCommon.publishMessage(pub, topic, 5, 10, qos, retained);
            // Subs connect again and check sub messages
            pubMessages.clear();
            for (MqttConnection sub : subList) {
                sub.connect();
                if (cleanSession) {
                    PubSubCommon.subscribe(sub, topic, qos);
                }
            }
            if (cleanSession && retained) {
                pubMessages.add(saveMessages.get(saveMessages.size() - 1));
            }
            
            if (qos == 1 && !cleanSession) {
                pubMessages.addAll(saveMessages);  // pub msgs qos=0 will not be stored when clean session is false
            }
            pubMessages.addAll(PubSubCommon.publishMessage(pub, topic, 10, 15, qos, retained));
            for (MqttConnection sub : subList) {
                log.info("Check persistent msg of client {} with broker url {}", sub.getClient().getClientId(),
                        sub.getClient().getServerURI());
                List<String> subMessages = sub.getCallback().waitAndGetReveiveList(pubMessages.size());
                PubSubCommon.checkCleanSessionResult(new ArrayList<String>(), pubMessages, subMessages, qos,
                        cleanSession);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (retained) {
                PubSubCommon.publish(pub, topic, 1, "", true);
            }
            if (null != pub) {
                pub.disconnect();
            }
            for (MqttConnection sub : subList) {
                PubSubCommon.unsubscribe(sub, topic);
                sub.disconnect();
            }
        }
    }

    /**
     * TestGoal: Test clean session feature of same sub.
     *
     * Main steps:
     *  Step1: Connect one sub with clean session = true/false and subscribe given topic. Then disconnect
     *   without unsubscribing.
     *  Step2: Connect one pub and publish a few msgs. Connect subs again and check received msgs according
     *   to clean session value.
     *  Step3: Diconnect and connect sub again. Check it will receive no msgs.
     *
     * @throws Exception
     */
    @Test
    public void testCleanSessionWithSameSub() throws Exception {
        Assume.assumeTrue(!cleanSession && !retained);
        String topic = String.format("cleanSession2%s", System.currentTimeMillis());

        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                offlineEdgePassword);
        conOptions.setCleanSession(cleanSession);
        MqttConnection sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Sub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        sub.setCallBack(new PubSubCallback());
        
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        try {
            // Sub connects, subscribes and disconnects
            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);
            sub.disconnect();

            // Pub connects and publishes 5 messages
            pub.connect();
            List<String> saveMessages = PubSubCommon.publishMessage(pub, topic, 0, 5, qos, retained);
            Thread.sleep(SLEEP_TIME);
            
            // Sub changes protocol and connects again and check sub messages
            String clientId = sub.getClient().getClientId();
            ConnectionType otherType = ConnectionType.getAnotherType(connectionType);
            boolean anotherTsl = ConnectionType.WSS.equals(otherType) || ConnectionType.SSL.equals(otherType)
                    ? true : false;
            log.info("Changing protocol to {}", otherType.toString());
            sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                    otherType, offlineEdgePortMap), clientId, anotherTsl, edgeCertPath, conOptions);
            sub.setCallBack(new PubSubCallback());
            sub.connect();
            PubSubCommon.checkCleanSessionResult(qos == 0 ? new ArrayList<String>() : saveMessages,
                    new ArrayList<String>(), sub.getCallback()
                    .waitAndGetReveiveList(saveMessages.size()), qos, cleanSession);

            // Same sub disconnects and connects again, should receive no message
            sub.disconnect();
            Thread.sleep(SLEEP_TIME);
            sub.connect();
            Assert.assertEquals("Duplicate persistent msg", 0, sub.getCallback()
                    .waitAndGetReveiveList(saveMessages.size(), 3).size());

            // Sub unsubscribe this topic, then disconnect
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();

            // Pub publish a few msg
            PubSubCommon.publishMessage(pub, topic, 5, 10, qos, retained);
            Thread.sleep(SLEEP_TIME);

            // Connect sub and check no msgs are received
            sub.connect();
            Assert.assertEquals("Unexpected msgs after unsubscribe", 0, sub.getCallback()
                    .waitAndGetReveiveList(saveMessages.size(), 3).size());
        } catch (Exception e) {
            throw e;
        } finally {
            pub.disconnect();
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
        }
    }
    
    /**
     * TestGoal: Test clean session msgs will not be received when sub changing its client id.
     *
     * @throws Exception
     */
    @Test
    public void testCleanSessionWithChangeClientId() throws Exception {
        Assume.assumeTrue(!cleanSession && !retained && qos == 1);

        Assume.assumeTrue(!cleanSession && !retained);
        String topic = "cleanSession3";

        MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                offlineEdgePassword);
        conOptions.setCleanSession(cleanSession);
        MqttConnection sub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Sub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, conOptions);
        sub.setCallBack(new PubSubCallback());
        
        MqttConnection pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls,
                edgeCertPath, connectOptions);
        
        MqttConnection anotherSub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), String.format("AnotherSub_%s", System.currentTimeMillis()),
                tls, edgeCertPath, conOptions);
        anotherSub.setCallBack(new PubSubCallback());
        try {
            // Connct a sub with clean session=fasle
            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);
            sub.disconnect();

            // Connect a pub and publish a few msgs
            pub.connect();
            List<String> saveMessages = PubSubCommon.publishMessage(pub, topic, 0, 5, qos, retained);
            Thread.sleep(SLEEP_TIME);
            
            // Connect another client and sub this topic
            anotherSub.connect();
            PubSubCommon.subscribe(anotherSub, topic, qos);
            Assert.assertEquals("Unexpected persistent msg", 0, anotherSub.getCallback()
                    .waitAndGetReveiveList(saveMessages.size(), 3).size());          
        } catch (Exception e) {
            throw e;
        } finally {
            pub.disconnect();
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
            
            PubSubCommon.unsubscribe(anotherSub, topic);
            anotherSub.disconnect();
        }
    }
    
    /**
     * TestGoal: Test retain feature of mqtt broker(Parameterized).
     *
     * Main steps:
     *  Step1: Connect one pub and publish a few msgs with retain = true/false.
     *  Step2: Connect a few subs and subscribe given topic. Check received msg with retain value. 
     *  Step2: Disconnect subs and connect again. Subscribe given topic and check received msg with retain value.
     *   (Retain msg should be received after each subscribe event).
     *  Step3: Disconnect subs again. Publish empty msg to topic with retain = true to clean retain msg. Then connect
     *   and subscribe to check no msg is received.
     *
     * @throws Exception
     */
    @Test
    public void testRetain() throws Exception {
        Assume.assumeTrue(cleanSession);   // Has nothing to do with clean session
        String topic = "_Retain";
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
        pub.connect();
        List<String> pubMessages = PubSubCommon.publishMessage(pub, topic, 0, 5, qos, retained);

        MqttConnection sub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, connectOptions);
        sub.setCallBack(new PubSubCallback());
        sub.connect();
        PubSubCommon.subscribe(sub, topic, qos);
        try {
            PubSubCommon.checkRetainResult(pubMessages, sub.getCallback(), qos, retained, true);

            // Disconnect and connect again, check still can receive retain msg
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
            Thread.sleep(2000);

            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);
            
            PubSubCommon.checkRetainResult(pubMessages, sub.getCallback(), qos, retained, true);
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
            Thread.sleep(2000);

            // Change retain msg
            pubMessages = PubSubCommon.publishMessage(pub, topic, 5, 8, qos, retained);
            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);
            PubSubCommon.checkRetainResult(pubMessages, sub.getCallback(), qos, retained, true);
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
            Thread.sleep(2000);
            
            // Clean retain msg
            PubSubCommon.publish(pub, topic, qos, "", true);
            Thread.sleep(1000);

            sub.connect();
            PubSubCommon.subscribe(sub, topic, qos);
            Assert.assertEquals("Receive unexpected retain msg", 0, sub.getCallback().waitAndGetReveiveList(1, 2)
                    .size());
        } catch (Exception e) {
            throw e;
        } finally {
            if (retained) {
                PubSubCommon.publish(pub, topic, 1, "", true);
            }
            pub.disconnect();
            PubSubCommon.unsubscribe(sub, topic);
            sub.disconnect();
        }
    }   
    
    /**
     * TestGoal: Test last will feature of mqtt broker.
     *
     * Main steps:
     *  Step1: Connect a few subs and subscribe the last will topic.
     *  Step2: Connect one pub and disconnect normally. Check no msgs will be forwarded.
     *  Step3: Connect again and disconnect it abormally(Force to stop thread and it will not send
     *   DISCONNECT package).
     *  Step4: Check each sub can receive last will msg.
     *
     * @throws Exception
     */
    @Test
    public void testLastWill() throws Exception {
        Assume.assumeTrue(cleanSession && !retained);   // Has nothing to do with clean session and retain
        // Change protocol if is SSL because of paho bug
        if (connectionType.equals(ConnectionType.SSL)) {
            connectionType = ConnectionType.WSS;
            log.info("Changing type to {}", connectionType.toString());
        }
        String topic = "last-will" + System.currentTimeMillis();
        String willMessage = "die" + System.currentTimeMillis();

        List<MqttConnection> subList = new ArrayList<MqttConnection>();
        MqttConnection pub = null;
        try {
            MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                    offlineEdgePassword);
            conOptions.setWill(topic, willMessage.getBytes(), qos, false);

            // Start subs
            for (int i = 0; i < random.nextInt(3) + 2; i++) {
                MqttConnection sub = PubSubCommon.createMqttConnection(
                        PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                        String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, conOptions);
                sub.setCallBack(new PubSubCallback());
                sub.connect();
                PubSubCommon.subscribe(sub, topic, qos);
                subList.add(sub);
            }

            pub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType,
                    offlineEdgePortMap), String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath,
                    conOptions);
            pub.connect();
            // Disconnect normally and check receive nothing
            pub.disconnect();
            Thread.sleep(SLEEP_TIME);
            for (MqttConnection sub : subList) {
                log.info("Check last will msg of client {} with broker url {}", sub.getClient().getClientId(),
                      sub.getClient().getServerURI());
                PubSubCommon.checkWillResult(null, sub.getCallback().getReceiveList(), qos);
            }
            // Stop pub thread forcibly
            pub.connect();
            Thread.sleep(SLEEP_TIME);
            PubSubCommon.stopConnectionForciblyWithoutWait(pub);

            for (MqttConnection sub : subList) {
                log.info("Check last will msg of client {} with broker url {}", sub.getClient().getClientId(),
                      sub.getClient().getServerURI());
                PubSubCommon.checkWillResult(willMessage, sub.getCallback(), qos, false);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            for (MqttConnection sub : subList) {
                PubSubCommon.unsubscribe(sub, topic);
                sub.disconnect();
            }
        }
    }

    /**
     * TestGoal: Test last will retain feature of mqtt broker.
     *
     * Main steps:
     *  Step1: Connect one pub, set last will msg with retain = true. Then disconnect it abormally
     *  (Force to stop thread and it will not send DISCONNECT package).
     *  Step2: Connect a few subs and subscribe the last will topic.
     *  Step3: Check each sub can receive last will msg.
     *
     * @throws Exception
     */
    @Test
    public void testLastWillRetain() throws Exception {
        Assume.assumeTrue(cleanSession && !retained);   // Has nothing to do with clean session and retain
        // Change protocol if is SSL because of paho bug
        if (connectionType.equals(ConnectionType.SSL)) {
            connectionType = ConnectionType.WSS;
            log.info("Changing type to {}", connectionType.toString());
        }
        String topic = "/last/will/retain/";
        String willMessage = "die" + System.currentTimeMillis();

        List<MqttConnection> subList = new ArrayList<MqttConnection>();

        MqttConnection pub = null;
        try {
            MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                    offlineEdgePassword);
            conOptions.setWill(topic, willMessage.getBytes(), qos, true);

            pub = PubSubCommon.createMqttConnection(
                    PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                    String.format("Pub_%s", System.currentTimeMillis()), tls, edgeCertPath, conOptions);
            pub.connect();
            PubSubCommon.publish(pub, topic, 1, "", true);   // Clear retain
            PubSubCommon.publishMessage(pub, topic, 0, 5, qos, false);
            Thread.sleep(1000);

            // Stop pub thread forcibly
            log.info("Stop connection without disconnect package");
            PubSubCommon.stopConnectionForciblyWithoutWait(pub);
            Thread.sleep(1000);

            // Start subs
            for (int i = 0; i < random.nextInt(2) + 2; i++) {
                MqttConnection sub = PubSubCommon.createMqttConnection(
                        PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                        String.format("Sub_%s", System.currentTimeMillis()), tls, edgeCertPath, conOptions);
                sub.setCallBack(new PubSubCallback());
                sub.connect();
                PubSubCommon.subscribe(sub, topic, qos);
                subList.add(sub);
            }

            for (MqttConnection sub : subList) {
                log.info("Check last will retain msg of client {} with broker url {}", sub.getClient().getClientId(),
                        sub.getClient().getServerURI());
                PubSubCommon.checkWillResult(willMessage, sub.getCallback(), qos, true);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            for (MqttConnection sub : subList) {
                PubSubCommon.unsubscribe(sub, topic);
                sub.disconnect();
            }
        }
    }
}
