package function;


import test.EDGEIntegrationTest;
import utils.Converter;
import utils.PubSubCommon;
import utils.client.ConnectionType;
import utils.client.EdgeDeviceMQTTChecker;
import utils.client.MqttConnection;
import utils.client.PubSubCallback;
import utils.client.RandomNameHolder;
import utils.msg.EdgeDeviceMsgForComplexSelect;
import utils.msg.EdgeDeviceNestMsg;
import utils.msg.EdgeDeviceSimpleMsg;
import utils.msg.RuleDataMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for sql handler of edge subscriptions.
 *
 * TestType: INTEGRATION_TEST
 *
 * IcafeId: bce-iot-4825
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceReSQLTest extends EDGEIntegrationTest {
    
    public ConnectionType connectionType;
    public boolean tls;
    public int qos;
    public static MqttConnectOptions connectOptions;
    public static EdgeDeviceMQTTChecker checker;
    public static long toleranceTime = 10000;  // 10s

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
     * TestGoal: Test select clause of many attributes in subscriptions 
     *
     * @throws Exception
     */
    @Test
    public void testSelectMany() throws Exception {
        // TODO bce-iot-4875 [Bug], bool definition is integer now
        // select device as `key`, `key` AS `value`, `value` as data, boolValue, * where `value` >= 10
        String sourceTopic = "selectMany";
        String targetTopic = "result/select/many";
        checker.bindTopic(targetTopic);
        checker.startSub();
        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("%s_%s", testName.getMethodName(), System.currentTimeMillis()), tls, edgeCertPath,
                connectOptions);
        
        int msgCount = 2 + random.nextInt(3);
        log.info("Publish {} correct and wrong msgs seperately", msgCount);
        List<String> expectedMsgs = new ArrayList<String>();
        // Correct msgs
        for (int i = 0; i < msgCount; i++) {
            EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg("device" + System.currentTimeMillis(),
                    RandomNameHolder.getRandomString(5), 10 + random.nextInt(5), random.nextBoolean(),
                    random.nextDouble());
            Map<String, Object> pubMsgMap = Converter.modelToMap(msg);
            if (i != 0) {
                String key = RandomNameHolder.getRandomString("key", 5);
                pubMsgMap.put("key", key);
            }
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(pubMsgMap), false);
            Thread.sleep(200);

            EdgeDeviceMsgForComplexSelect resultMsg = new EdgeDeviceMsgForComplexSelect(msg.getDevice(),
                    String.valueOf(pubMsgMap.get("key")), String.valueOf(msg.getValue()), msg.isBoolValue() ? 1 : 0);
            expectedMsgs.add(Converter.modelToJsonUsingJsonNode(resultMsg));
        }
        
        // Wrong msgs
        for (int i = 0; i < msgCount; i++) {
            EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg("device" + System.currentTimeMillis(),
                    RandomNameHolder.getRandomString(5), 9 - random.nextInt(5), random.nextBoolean(),
                    random.nextDouble());
            String key = RandomNameHolder.getRandomString("key", 5);
            Map<String, Object> pubMsgMap = Converter.modelToMap(msg);
            pubMsgMap.put("key", key);
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(pubMsgMap), false);
        }
        
        checker.checkResultMessage(targetTopic, expectedMsgs);
        pub.disconnect();
    }

    /**
     * TestGoal: Test normal sql functions in select clause of subscriptions
     *
     * @throws Exception
     */
    @Test
    public void testSelectFunctions() throws Exception {
        // select `value` + 1 as `value`, doubleValue * `value` as doubleValue, ceil(doubleValue) as ceilValue,
        // floor(doubleValue) as floorValue, mod(doubleValue, `value`) as modValue where device = 'device_abc'
        String sourceTopic = "selectFunctions";
        String targetTopic = "result/select/function";
        checker.bindTopic(targetTopic);
        checker.startSub();
      
        String pubClient = "device_abc";        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                pubClient, tls, edgeCertPath, connectOptions);
        
        int msgCount = 2 + random.nextInt(3);
        log.info("Publish {} correct msgs and wrong msgs seperately", msgCount);
        List<String> expectedMsgs = new ArrayList<String>();
        for (int i = 0; i < msgCount; i++) {
            int intValue =  10 + random.nextInt(5);
            double doubleValue = random.nextDouble() * + random.nextInt(100);
            EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg(pubClient,
                    RandomNameHolder.getRandomString(5), intValue, random.nextBoolean(),
                    doubleValue);
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(msg), false);

            Map<String, Object> expectedMsgMap = new HashMap<String, Object>();
            expectedMsgMap.put("value", intValue + 1);
            expectedMsgMap.put("doubleValue", doubleValue * intValue);
            expectedMsgMap.put("ceilValue", (int) Math.ceil(doubleValue));
            expectedMsgMap.put("floorValue", (int) Math.floor(doubleValue));
            expectedMsgMap.put("modValue", doubleValue % intValue);
            expectedMsgs.add(Converter.modelToJsonUsingJsonNode(expectedMsgMap));
            Thread.sleep(200);
        }
        
        // Wrong msgs
        for (int i = 0; i < msgCount; i++) {
            EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg("device" + System.currentTimeMillis(),
                    RandomNameHolder.getRandomString(5), 9 - random.nextInt(5), random.nextBoolean(),
                    random.nextDouble());
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(msg), false);
            Thread.sleep(200);
        }
        
        checker.checkResultMessage(targetTopic, expectedMsgs);
        pub.disconnect();    
    }

    /**
     * TestGoal: Test customized sql functions in select clause of subscriptions(mainly about mqtt features)
     *
     * @throws Exception
     */
    @Test
    public void testCustomSelectFunctions() throws Exception {
        // clientid() AS device, clientip() AS data, topic() as topic, qos() as value
        String sourceTopic = "custom/functions";
        String targetTopic = "result/custom/function";
        checker.bindTopic(targetTopic);
        checker.startSub();
        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                "pub" + System.currentTimeMillis(), tls, edgeCertPath, connectOptions);
        
        int msgCount = 2 + random.nextInt(3);
        log.info("Publish {} correct msgs", msgCount);
        List<String> expectedMsgs = new ArrayList<String>();
        for (int i = 0; i < msgCount; i++) {
            String msgStr = EMPTY_MAP_STRING;
            if (random.nextBoolean()) {
                EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg();
                msgStr = Converter.modelToJson(msg);
            }
            PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);

            Map<String, Object> expectedMsgMap = new HashMap<String, Object>();
            // TODO client ip are null, qos is always 0 now
            expectedMsgMap.put("topic", sourceTopic);
            expectedMsgMap.put("value", 0);
            expectedMsgs.add(Converter.modelToJsonUsingJsonNode(expectedMsgMap));
        }
        
        checker.checkResultMessage(targetTopic, expectedMsgs);
        pub.disconnect();
    }

    /**
     * TestGoal: Test customized sql functions in select clause of subscriptions(mainly about time)
     *
     * @throws Exception
     */
    @Test
    public void testCustomSelectFunctions2() throws Exception {
        // select CURRENT_TIMESTAMP AS current, LOCALTIMESTAMP AS local, uuid() as uuid, newid() as newId
        int qos = random.nextInt(2);
        String sourceTopic = "custom/functions2";
        String targetTopic = "result/custom/function2";
        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                "pub" + System.currentTimeMillis(), tls, edgeCertPath, connectOptions);
        MqttConnection sub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                "sub" + System.currentTimeMillis(), tls, edgeCertPath, connectOptions);
        sub.setCallBack(new PubSubCallback());
        sub.connect();
        PubSubCommon.subscribe(sub, targetTopic, qos);
        int msgCount = 2 + random.nextInt(2);
        log.info("Publish {} correct msgs", msgCount);
        for (int i = 0; i < msgCount; i++) {
            String msgStr = EMPTY_MAP_STRING;
            if (random.nextBoolean()) {
                EdgeDeviceSimpleMsg msg = new EdgeDeviceSimpleMsg();
                msgStr = Converter.modelToJson(msg);
            }
            PubSubCommon.publish(pub, sourceTopic, qos, msgStr, false);
            List<String> receivedMsgs = sub.getCallback().waitAndGetReveiveList(1);
            Map<String, Object> resultMap = Converter.jsonToModel(receivedMsgs.get(0), Map.class);
            long currentTimestamp = Long.valueOf(String.valueOf(resultMap.get("current")));
            long localTimestamp = Long.valueOf(String.valueOf(resultMap.get("local")));
            String uuid = String.valueOf(String.valueOf(resultMap.get("uuid")));
            String newId = String.valueOf(String.valueOf(resultMap.get("newId")));

            Assert.assertTrue("Received [CURRENT_TIMESTAMP] not match expect!",
                    Math.abs(System.currentTimeMillis() - currentTimestamp) < toleranceTime);
            long expectedLocalTimestamp = System.currentTimeMillis();
            Assert.assertTrue("Received [LOCAL_TIMESTAMP] not match expect!",
                    Math.abs(localTimestamp - expectedLocalTimestamp) < toleranceTime);
            Assert.assertNotNull(uuid);
            Assert.assertNotNull(newId);
            Assert.assertNotEquals(uuid, newId);
            Thread.sleep(1000);
        }
        
        sub.disconnect();
        pub.disconnect();
    }

    /**
     * TestGoal: Test filter feature of where clause in subscriptions.
     *
     * @throws Exception
     */
    @Test
    public void testNormalWhere() throws Exception {
        // test1: select * where ce='eq' AND cne <> 'eq' AND ((cv1 <= 10 OR cv2 > cv1 + 10) OR cv3 IN ('expectA', 'expectB'))
        // test2: select * where cv3 like 'expect%' AND abs(cv1) > 10
        qos = 1; // For order
        String sourceTopic = "where/test";
        String targetTopic1 = "result/where/test1";
        String targetTopic2 = "result/where/test2";
        checker.bindTopic(targetTopic1);
        checker.bindTopic(targetTopic2);
        checker.startSub();
      
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                "wheretest" + System.currentTimeMillis(), tls, edgeCertPath, connectOptions);
        
        List<RuleDataMsg> dataMsgs = Arrays.asList(
                new RuleDataMsg("TOPIC_A", "eq", "neq", 10, 17, "expectA"), // 0
                new RuleDataMsg("TOPIC_B", "eq", "neq", 11, 22, "expectB"), // 1
                new RuleDataMsg("TOPIC_C", "eq", "neq", 7, 2, "expectA"),  // 2
                new RuleDataMsg("TOPIC_D", "eq", "neq", 10, 22, "expectD"), // 3
                new RuleDataMsg("TOPIC_E", "neq", "neq", 11, 22, "expectA"),// 4
                new RuleDataMsg("TOPIC_F", "eq", "neq", 11, 21, "expectF"),// 5
                new RuleDataMsg("TOPIC_G", "eq", "neq", 10, 17, "expectE"),// 6
                new RuleDataMsg("TOPIC_H", "eq", "eq", 10, 22, "expectF"),// 7
                new RuleDataMsg("TOPIC_I", "eq", "eq", 15, 22, "WRONG")// 8
        );
        
        List<Integer> matchedIndexForTarget1 = Arrays.asList(0, 1, 2, 3, 6);
        List<String> matchedMsgForTarget1 = new ArrayList<String>();
        for (int index : matchedIndexForTarget1) {
            matchedMsgForTarget1.add(Converter.modelToJsonUsingJsonNode(dataMsgs.get(index)));
        }
        
        List<Integer> matchedIndexForTarget2 = Arrays.asList(1, 4, 5);
        List<String> matchedMsgForTarget2 = new ArrayList<String>();
        for (int index : matchedIndexForTarget2) {
            matchedMsgForTarget2.add(Converter.modelToJsonUsingJsonNode(dataMsgs.get(index)));
        }
        
        for (RuleDataMsg msg : dataMsgs) {
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(msg), false);
            Thread.sleep(1000);
        }
        checker.checkResultMessage(targetTopic1, matchedMsgForTarget1);
        checker.checkResultMessage(targetTopic2, matchedMsgForTarget2);
        pub.disconnect();
    }

    /**
     * TestGoal: Test msg forward feature of subscriptions which select and where clause having nested format.
     *
     * @throws Exception
     */
    @Test
    public void testSubscriptionWithNestDefinition() throws Exception {
        // select nestMsg.device AS `key`, nestMsg.`value` AS `value` where nestMsg.`value` <> 5
        String sourceTopic = "nestmsg/test";
        String targetTopic = "result/nestmsg";
        checker.bindTopic(targetTopic);
        checker.startSub();
        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                "wheretest" + System.currentTimeMillis(), tls, edgeCertPath, connectOptions);
        // Correct msg
        EdgeDeviceSimpleMsg innerMsg = new EdgeDeviceSimpleMsg(pub.getClient().getClientId(),
                testName.getMethodName() + System.currentTimeMillis(), random.nextInt(), random.nextBoolean(),
                random.nextDouble());
        EdgeDeviceNestMsg nestMsg = new EdgeDeviceNestMsg(RandomNameHolder.getRandomString(5), "" + System
                .currentTimeMillis(), innerMsg);
        PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(nestMsg), false);
        
        // Wrong msg
        EdgeDeviceSimpleMsg wrongInnerMsg = new EdgeDeviceSimpleMsg(pub.getClient().getClientId(),
                testName.getMethodName() + System.currentTimeMillis(), 5, false, random.nextDouble());
        EdgeDeviceNestMsg wrongNestMsg = new EdgeDeviceNestMsg();
        wrongNestMsg.setNestMsg(wrongInnerMsg);
        PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(wrongNestMsg), false);
        PubSubCommon.publish(pub, sourceTopic, qos, EMPTY_MAP_STRING, false);
        Thread.sleep(SLEEP_TIME);

        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("key", innerMsg.getDevice());
        resultMap.put("value", innerMsg.getValue());
        checker.checkResultMessage(targetTopic, Collections.singletonList(Converter
                .modelToJsonUsingJsonNode(resultMap)));
        pub.disconnect();
    }
}
