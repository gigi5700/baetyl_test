package remote;

import test.EDGEIntegrationTest;
import utils.PubSubCommon;
import utils.client.ConnectionType;
import utils.client.MqttConnection;
import utils.client.PubSubCallback;
import utils.client.RandomNameHolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Test cases for remote features of edge device.
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceRemoteFeatureTest extends EDGEIntegrationTest {
    
    public ConnectionType connectionType;
    public ConnectionType connectionTypeForRemote = ConnectionType.TCP;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions edgeConnectOptions;
    public static MqttConnectOptions hubAllTopiConnectOptions;
    public static MqttConnectOptions hubSpecificTopicConnectOptions;
    public int reconnectTimeout = 60;
    public static String prefix = "/docker";

    @Before
    public void setUp() throws Exception {
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL.equals(connectionType) ? true : false;
        qos = 1;
        log.info("Connection type is {}", connectionType.toString());
        edgeConnectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
        hubAllTopiConnectOptions = PubSubCommon.getDefaultConnectOptions(remoteUsernameWithAllTopic, remotePwdWithAllTopic);
        hubSpecificTopicConnectOptions = PubSubCommon.getDefaultConnectOptions(remoteUsernameWithSpecificTopic,
                remotePwdWithSpecificTopic);
    }

    /**
     * TestGoal: Test subscriptions which sources are remote topics containing wildcard characters.
     *
     * @throws Exception
     */
    @Test
    public void testRemoteWithWildcardTopics() throws Exception {
//        hub:
//        subscriptions:
//          - topic: remote/sharp/#

//        remote:
//        cleansession: true
//        subscriptions:
//          - topic: remote/plus/+
        String sharpTopicPrefix = String.format("remote%s/sharp", prefix);
        String plusTopicPrefix = String.format("remote%s/plus", prefix);
        List<String> correctTopicsForSharp = Arrays.asList(sharpTopicPrefix + "/A/B", sharpTopicPrefix + "/A",
                sharpTopicPrefix);
        List<String> correctTopicsForPlus = Arrays.asList(plusTopicPrefix + "/A", plusTopicPrefix + "/B",
                plusTopicPrefix + "/");

        List<String> wrongTopicsForSharp = Collections.singletonList("remote" + prefix);
        List<String> wrongTopicsForPlus = Arrays.asList(plusTopicPrefix + "/A/B", plusTopicPrefix);

        MqttConnection remoteCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionTypeForRemote, remoteHubPortMap), "wildcard" + System.currentTimeMillis(),
                false,  edgeCertPath, hubAllTopiConnectOptions);
        remoteCon.setCallBack(new PubSubCallback());

        MqttConnection edgeHubCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "edgeSub" + System.currentTimeMillis(), tls, edgeCertPath,
                edgeConnectOptions);
        edgeHubCon.setCallBack(new PubSubCallback());

        try {
            edgeHubCon.connect();
            PubSubCommon.subscribe(edgeHubCon, correctTopicsForPlus, qos);
            PubSubCommon.subscribe(edgeHubCon, wrongTopicsForPlus, qos);

            remoteCon.connect();
            PubSubCommon.subscribe(remoteCon, correctTopicsForSharp, qos);
            PubSubCommon.subscribe(remoteCon, wrongTopicsForSharp, qos);

            // For sharp
            for (String topic : correctTopicsForSharp) {
                String msgStr = topic + RandomNameHolder.getRandomString(5);
                PubSubCommon.publish(edgeHubCon, topic, qos, msgStr, false);
                Assert.assertEquals(String.format("Topic %s received wrong msgs", topic), Collections
                        .singletonList(msgStr), remoteCon.getCallback().waitAndGetReveiveListMap(topic, 1));
            }
            remoteCon.getCallback().clear();

            for (String topic : wrongTopicsForSharp) {
                String msgStr = topic + RandomNameHolder.getRandomString(5);
                PubSubCommon.publish(edgeHubCon, topic, qos, msgStr, false);
            }
            Assert.assertEquals("Received unexpected msgs", 0, remoteCon.getCallback().waitAndGetReveiveList(1, 3)
                    .size());

            // For plus
            for (String topic : correctTopicsForPlus) {
                String msgStr = topic + RandomNameHolder.getRandomString(5);
                PubSubCommon.publish(remoteCon, topic, qos, msgStr, false);
                Assert.assertEquals(String.format("Topic %s received wrong msgs", topic), Collections
                        .singletonList(msgStr), edgeHubCon.getCallback().waitAndGetReveiveListMap(topic, 1));
            }
            edgeHubCon.getCallback().clear();

            for (String topic : wrongTopicsForPlus) {
                String msgStr = topic + RandomNameHolder.getRandomString(5);
                PubSubCommon.publish(remoteCon, topic, qos, msgStr, false);
            }
            Assert.assertEquals("Received unexpected msgs", 0, edgeHubCon.getCallback().waitAndGetReveiveList(1, 3)
                    .size());
        } catch (Exception e) {
            throw e;
        } finally {
            remoteCon.disconnect();
            edgeHubCon.disconnect();
        }
    }

    /**
     * TestGoal: Test clean session feature of sub client in remote rules.
     *
     * @throws Exception
     */
    @Test
    public void testRemoteWithCleanSession() throws Exception {
//        Feature1: msg persistent in edge
//        hub:
//
//        remote:
//        cleansession: true
//        subscriptions:
//          - topic: remote/clean_session

        String pubTopic = String.format("remote%s/clean_session", prefix);
        qos = 1;
        MqttConnection remotePub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionTypeForRemote, remoteHubPortMap), "remotePub" + System.currentTimeMillis(),
                false,  edgeCertPath, hubSpecificTopicConnectOptions);

        MqttConnectOptions edgeFalseConnectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername,
                offlineEdgePassword);
        edgeFalseConnectOptions.setCleanSession(false);

        MqttConnection edgeSub = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "edgeSub" + System.currentTimeMillis(),
                tls, edgeCertPath, edgeFalseConnectOptions);
        edgeSub.setCallBack(new PubSubCallback());

        try {
            // Connect one edge client and subscribe target topic with clean session=false, then disconnect
            edgeSub.connect();
            PubSubCommon.subscribe(edgeSub, pubTopic, qos);
            edgeSub.disconnect();

            // Publish to source topic
            remotePub.connect();
            List<String> pubMsgs = PubSubCommon.publishMessage(remotePub, pubTopic, 0, 5, qos, false);
            Thread.sleep(1000);

            // Connect edge client again and check
            edgeSub.connect();
            PubSubCommon.checkPubAndSubResult(pubMsgs, edgeSub.getCallback().waitAndGetReveiveListMap(pubTopic,
                    pubMsgs.size()), qos);
        } catch (Exception e) {
            throw e;
        } finally {
            remotePub.disconnect();
            PubSubCommon.unsubscribe(edgeSub, pubTopic);
            edgeSub.disconnect();
        }
    }

    /**
     * TestGoal: Test remote features with topic auth check.
     *
     * Main steps:
     *  Step1: There are two remotes. A has permissions of all topics(allAuth) and the B only has permissions of
     *   remote/#(noAuth). Subscriptions config of rules are like comments below.
     *  Step2: Local hub publish msgs to both authorized and unauthorized topics for remote B. Check connection of
     *   remote B can only receive msgs from authorized topic and connection of remote A can receive both.
     *  Step3: Connection of remote B publish msgs to a topic and check local hub can receive these msgs from same
     *   topic with migrated qos.
     *
     * @throws Exception
     */
    @Test
    public void testRemoteWithAuthCheck() throws Exception {
//        hub:
//        subscriptions:
//        - topic: remote/pub qos: 0
//        - topic: unauthorized/pub qos: 0

//        remote:(noAuth)
//        cleansession: true
//        subscriptions:
//        - topic: remote/sub qos: 1

//        remote:(allAuth)
        String remoteAuthTopic = String.format("remote%s/pub", prefix);
        String noAuthTopicForRemoteCon = String.format("unauthorized%s/pub", prefix);
        String hubAuthTopic = String.format("remote%s/sub", prefix);
        int qos1 = 1;

        MqttConnection remoteCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionTypeForRemote, remoteHubPortMap), "remotePub" + System.currentTimeMillis(), false,
                edgeCertPath, hubSpecificTopicConnectOptions);
        remoteCon.setCallBack(new PubSubCallback());

        MqttConnection remoteConWithAllAuth = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionTypeForRemote, remoteHubPortMap), "remotePub" + System.currentTimeMillis(),
                false,  edgeCertPath, hubAllTopiConnectOptions);
        remoteConWithAllAuth.setCallBack(new PubSubCallback());

        MqttConnection localHubCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "pub" + System.currentTimeMillis(), tls, edgeCertPath,
                edgeConnectOptions);
        localHubCon.setCallBack(new PubSubCallback());

        try {
            remoteCon.connect();
            PubSubCommon.subscribe(remoteCon, Arrays.asList(remoteAuthTopic, noAuthTopicForRemoteCon), qos1);
            remoteConWithAllAuth.connect();
            PubSubCommon.subscribe(remoteConWithAllAuth, noAuthTopicForRemoteCon, qos1);
            Thread.sleep(SLEEP_TIME);

            localHubCon.connect();
            PubSubCommon.subscribe(localHubCon, hubAuthTopic, qos1);

            // Local hub connection publishes a few msgs
            log.info("Publish one no auth msg");
            String noAuthMsg = "noAuth" + System.currentTimeMillis();
            PubSubCommon.publish(localHubCon, noAuthTopicForRemoteCon, qos1, noAuthMsg, false);
            Thread.sleep(200);

            int msgCount = 2 + random.nextInt(3);
            log.info("Publish {} auth msgs", msgCount);
            List<String> pubMsgs = new ArrayList<>();
            for (int i = 0; i < msgCount; i++) {
                String msg = String.format("auth%s_%s", i, System.currentTimeMillis());
                PubSubCommon.publish(localHubCon, remoteAuthTopic, random.nextInt(2), msg, false);
                Thread.sleep(200);
                pubMsgs.add(msg);
            }

            // Check sub result and qos
            Assert.assertEquals("Remote sub received wrong msgs", pubMsgs, remoteCon.getCallback()
                    .waitAndGetReveiveListMap(remoteAuthTopic, pubMsgs.size(), reconnectTimeout + 10));
            Assert.assertEquals("Received msgs from unauthorized topic", 0, remoteCon.getCallback()
                    .waitAndGetReveiveListMap(noAuthTopicForRemoteCon, 1, 3).size());

            Assert.assertEquals("Remote sub with all authorities received wrong msgs", Collections
                            .singletonList(noAuthMsg),
                    remoteConWithAllAuth.getCallback().waitAndGetReveiveListMap(noAuthTopicForRemoteCon, 1));

            // Remote connection publishes a few msgs
            pubMsgs = new ArrayList<>();
            List<Integer> pubQosList = new ArrayList<>();
            for (int i = 0; i < msgCount; i++) {
                String msg = String.format("remote%s_%s", i, System.currentTimeMillis());
                int pubQos = pubQosList.size() == 0 ? 0 : 1 - pubQosList.get(pubQosList.size() - 1);
                PubSubCommon.publish(remoteCon, hubAuthTopic, pubQos, msg, false);
                log.info("Add pub qos {} to list", pubQos);
                pubQosList.add(pubQos);
                Thread.sleep(200);
                pubMsgs.add(msg);
            }
            Assert.assertEquals("Local sub received wrong msgs", pubMsgs, localHubCon.getCallback()
                    .waitAndGetReveiveListMap(hubAuthTopic, pubMsgs.size()));
        } catch (Exception e) {
            throw e;
        } finally {
            localHubCon.disconnect();
            PubSubCommon.unsubscribe(remoteCon, remoteAuthTopic);
            remoteCon.disconnect();
            PubSubCommon.unsubscribe(remoteConWithAllAuth, noAuthTopicForRemoteCon);
            remoteConWithAllAuth.disconnect();
        }
    }

    /**
     * TestGoal: Test remote features with remote having no sub authorizations.
     *
     * Main steps:
     *  Step1: Configs are below. Remote A has no permissions topic in subscriptions.
     *  Step2: Local hub publish msgs to the topic in config and check connection of remote A can receive these msgs
     *   from same topic with migrated qos.(hub->remote should not be impacted by this no auth topic)
     *  Step3: Remote connection having permissions of all topics publish msgs to the no auth topic in Step1. Check
     *   local hub can receive nothing.(remote->hub of this no auth topic should not work)
     *
     * @throws Exception
     */
    @Test
    public void testRemoteWithNoSubAuth() throws Exception {
//        hub:
//        subscriptions:
//        - topic: remote/auth qos: 0

//        remote:(noAuth)
//        subscriptions:
//        - topic: unauthorized/sub qos: 1
        String authTopicForRemoteCon = "remote/authcheck";
        String noAuthTopicForRemoteCon = String.format("unauthorized/sub%s", prefix);

        MqttConnection remoteConWithNoSubAuth = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(
                offlineEdgeUrl, connectionTypeForRemote, remoteHubPortMap), "remotePub" + System.currentTimeMillis(),
                false,  edgeCertPath, hubSpecificTopicConnectOptions);

        remoteConWithNoSubAuth.setCallBack(new PubSubCallback());

        MqttConnection edgeCon = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), "pub" + System.currentTimeMillis(),
                tls, edgeCertPath, edgeConnectOptions);
        edgeCon.setCallBack(new PubSubCallback());


        MqttConnection remoteConWithAllAuth = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionTypeForRemote, remoteHubPortMap), "remotePub" + System.currentTimeMillis(),
                false,  edgeCertPath, hubAllTopiConnectOptions);
        try {
            remoteConWithNoSubAuth.connect();
            edgeCon.connect();
            PubSubCommon.subscribe(edgeCon, noAuthTopicForRemoteCon, qos);
            PubSubCommon.subscribe(remoteConWithNoSubAuth, authTopicForRemoteCon, qos);

            // Check remote can receive msgs from localhub correctly
            List<String> pubMsgs = PubSubCommon.publishMessage(edgeCon, authTopicForRemoteCon, 0, 5, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsgs, remoteConWithNoSubAuth.getCallback().waitAndGetReveiveListMap(
                    authTopicForRemoteCon, pubMsgs.size()), qos);

            // Unauthorized sub topic for remote will not forward msg to local hub
            pubMsgs = PubSubCommon.publishMessage(remoteConWithAllAuth, noAuthTopicForRemoteCon, 0, 5, qos, false);
            Assert.assertEquals("Received msgs from unauthorized topic", 0, edgeCon.getCallback()
                    .waitAndGetReveiveListMap(noAuthTopicForRemoteCon, pubMsgs.size(), 3).size());
        } catch (Exception e) {
            throw e;
        } finally {
            remoteConWithNoSubAuth.disconnect();
            edgeCon.disconnect();
            remoteConWithAllAuth.disconnect();
        }
    }
}