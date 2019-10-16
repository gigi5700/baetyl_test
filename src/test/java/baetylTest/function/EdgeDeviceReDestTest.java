package baetylTest.function;

import baetylTest.EDGEIntegrationTest;
import baetylTest.utils.Converter;
import baetylTest.utils.PubSubCommon;
import baetylTest.utils.client.ConnectionType;
import baetylTest.utils.client.EdgeDeviceMQTTChecker;
import baetylTest.utils.client.EdgeRuleFunctionContext;
import baetylTest.utils.client.EdgeRuleHandlerKind;
import baetylTest.utils.client.MqttConnection;
import baetylTest.utils.client.RandomNameHolder;
import baetylTest.utils.msg.EdgeDeviceFunctionResultMsg;
import baetylTest.utils.msg.EdgeDeviceNestMsg;
import baetylTest.utils.msg.EdgeDeviceSimpleMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for destination check of subscriptions.
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceReDestTest extends EDGEIntegrationTest {

    public ConnectionType connectionType;
    public boolean tls;
    public int qos = 0;
    public static MqttConnectOptions connectOptions;
    public static EdgeDeviceMQTTChecker checker;

    @Before
    public void setUp() throws Exception {
        connectionType = ConnectionType.values()[random.nextInt(ConnectionType.values().length)];
        tls = ConnectionType.WSS.equals(connectionType) || ConnectionType.SSL
                .equals(connectionType) ? true : false;
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
     * TestGoal: All msgs should be forwarded correctly of a subscription with multiple dests and handlers
     *  but one same source topic.
     *
     * @throws Exception
     */
    @Test
    public void testSubscriptionWithMultiDest() throws Exception {
        String sourceTopic = "multi/test";
        String pythonFucitonTargetTopic = "result/python";
        String sqlTargetTopic = "result/sql";        // select device, topic() as data, *
        String mqttTargetTopic = "result/mqtt";
        Map<EdgeRuleHandlerKind, List<String>> handleToTopicMap = new HashMap<EdgeRuleHandlerKind, List<String>>();
        handleToTopicMap.put(EdgeRuleHandlerKind.MQTT, Collections.singletonList(mqttTargetTopic));
        handleToTopicMap.put(EdgeRuleHandlerKind.FUNCTION_PYTHON, Collections.singletonList(pythonFucitonTargetTopic));
        handleToTopicMap.put(EdgeRuleHandlerKind.SQL, Collections.singletonList(sqlTargetTopic));

        for (EdgeRuleHandlerKind kind : handleToTopicMap.keySet()) {
            for (String topic : handleToTopicMap.get(kind)) {
                checker.bindTopic(topic);
            }
        }
        checker.startSub();
        
        String pubClientId = "multiDest" + System.currentTimeMillis();
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, connectOptions);
        EdgeDeviceSimpleMsg correctMsg = new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System
                .currentTimeMillis());
        EdgeDeviceSimpleMsg wrongMsg = new EdgeDeviceSimpleMsg();
        wrongMsg.setData("wrong");
        
        EdgeRuleFunctionContext context = new EdgeRuleFunctionContext(pythonFunctionName, qos, sourceTopic, false);

        List<EdgeDeviceSimpleMsg> pubMsgs = Arrays.asList(correctMsg, wrongMsg);
        Map<EdgeRuleHandlerKind, List<String>> handleToExpectedMsgMap = new HashMap<EdgeRuleHandlerKind,
                List<String>>();
        for (EdgeDeviceSimpleMsg msg : pubMsgs) {
            String msgStr = Converter.modelToJsonUsingJsonNode(msg);
            PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);
            // Add mqtt result msg
           addExpectedMsgToResult(msgStr, EdgeRuleHandlerKind.MQTT, handleToExpectedMsgMap);
            
            // Add sql result msg
            Map<String, Object> msgMap = Converter.modelToMap(msg);
            msgMap.put("data", sourceTopic);
            msgMap.put("device", msg.getDevice());
            addExpectedMsgToResult(Converter.modelToJsonUsingJsonNode(msgMap), EdgeRuleHandlerKind.SQL,
                    handleToExpectedMsgMap);
            // Add python result msg
            EdgeDeviceFunctionResultMsg pythonResultMsg = new EdgeDeviceFunctionResultMsg(pythonFunctionUser, context);
            pythonResultMsg.setValue(String.valueOf(msg.getValue()));
            addExpectedMsgToResult(Converter.modelToJsonUsingJsonNode(pythonResultMsg), EdgeRuleHandlerKind
                    .FUNCTION_PYTHON, handleToExpectedMsgMap);
            Thread.sleep(200);
        }
        
        for (EdgeRuleHandlerKind kind : handleToTopicMap.keySet()) {
            log.info("Checking {} dest", kind.toString());
            for (String topic : handleToTopicMap.get(kind)) {
                checker.checkResultMessage(topic, handleToExpectedMsgMap.get(kind));
            }
        }
        pub.disconnect();
    }
    
    /**
     * TestGoal: All msgs should be forwarded correctly of a simple subscription with one single dest.
     *
     * @throws Exception
     */
    @Test
    public void testSubscriptionWithSingleDest() throws Exception {
        String sourceTopic = "single/test";
        String mqttTargetTopic = "single/result";
        checker.bindTopic(mqttTargetTopic);
        checker.startSub();
        
        String pubClientId = "singledest" + System.currentTimeMillis();
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, connectOptions);
        EdgeDeviceSimpleMsg innerMsg = new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System
                .currentTimeMillis());
        EdgeDeviceNestMsg nestMsg = new EdgeDeviceNestMsg(RandomNameHolder.getRandomString(5), "" + System
                .currentTimeMillis(), innerMsg);
        String msgStr = Converter.modelToJsonUsingJsonNode(nestMsg);
        PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);
        checker.checkResultMessage(mqttTargetTopic, Collections.singletonList(msgStr));
        pub.disconnect();
    }
    
    /**
     * TestGoal: Test msg forward feature of a subscription formating a chain.
     *
     * @throws Exception
     */
    @Test
    public void testChainSubscription() throws Exception {
        // chain/test/1 -> chain/test/2 -> $function/simple_sql -> $function/simple_python -> chain/test/3
        String sourceTopic = "chain/test/1";
        List<String> targetTopics = Arrays.asList("chain/test/2", "chain/test/3");
        for (String topic : targetTopics) {
            checker.bindTopic(topic);
        }
        checker.startSub();
        
        String pubClientId = "chain" + System.currentTimeMillis();
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, connectOptions);
        EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System
                .currentTimeMillis());
        String msgStr = Converter.modelToJsonUsingJsonNode(msg);
        PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);
        for (String topic : targetTopics) {
            checker.checkResultMessage(topic, Collections.singletonList(msgStr));
        }
        pub.disconnect();
    }

    /**
     * TestGoal: Test msg should be forwarded correctly bewteen different principals.
     *
     * @throws Exception
     */
    @Test
    public void testSubscriptionWithAcrossPrincipal() throws Exception {
        // across/test -> across/result(another principal)
        // across/result -> $function/simple_sql -> across/back(back to the origin one)
        String sourceTopic = "across/test";
        String targetOfAnotherPrincipal = "across/result";
        String targetOfBackResult = "across/back";
        checker.bindTopic(targetOfBackResult);
        checker.startSub();

        String pubClientId = "chain" + System.currentTimeMillis();
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, connectOptions);
        
        // Connection of another principal
        MqttConnectOptions anotherConOpts = PubSubCommon.getDefaultConnectOptions(offlineEdgeAnotherUsername,
                offlineEdgeAnotherPassword);
        MqttConnection another = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClientId, tls, edgeCertPath, anotherConOpts);
        EdgeDeviceMQTTChecker anotherCheck = new EdgeDeviceMQTTChecker();
        anotherCheck.bindClient(another);
        anotherCheck.bindTopic(targetOfAnotherPrincipal);
        anotherCheck.startSub();
        
        List<EdgeDeviceSimpleMsg> msgs = Arrays.asList(
                new EdgeDeviceSimpleMsg(pubClientId, testName.getMethodName() + System.currentTimeMillis()),
                new EdgeDeviceSimpleMsg());
        List<String> expecetedMsgs = new ArrayList<String>();
        for (EdgeDeviceSimpleMsg msg : msgs) {
            String msgStr = Converter.modelToJsonUsingJsonNode(msg);
            PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);
            Thread.sleep(200);
            expecetedMsgs.add(msgStr);
        }

        anotherCheck.checkResultMessage(targetOfAnotherPrincipal, expecetedMsgs);
        checker.checkResultMessage(targetOfBackResult, expecetedMsgs);
    }
}
