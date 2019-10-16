package baetylTest.utils.client;

import baetylTest.utils.CheckCommon;
import baetylTest.utils.Converter;
import baetylTest.utils.PubSubCommon;
import baetylTest.utils.msg.EdgeDeviceFunctionResultMsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Assert;

/**
 * MQTTChecker of edgeTestCases subscription
 *
 * @author Zhao Meng
 */
@Slf4j
@Data
public class EdgeDeviceMQTTChecker {

    public Map<String, MqttConnection> topicDevices = new HashMap<>();
    public MqttConnection sampleConnection;
    public static final String ERROR_TYPE = "errorType";
    public static final String NOT_NAMED_KEY = "NOT_NAMED_0";

    public void bindClient(Object client) {
        // Init sample connection
        sampleConnection = (MqttConnection) client;
    }

    public void bindTopic(String topic) throws Exception {
        if (!topicDevices.containsKey(topic)) {
            String username = sampleConnection.getConnOpts().getUserName();
            String password = new String(sampleConnection.getConnOpts().getPassword());
            MqttConnectOptions conOptions = PubSubCommon.getDefaultConnectOptions(username, password);
            String url = sampleConnection.getClient().getServerURI();
            String clientId = "mqttCheck" + UUID.randomUUID().toString();
            MqttConnection connection = PubSubCommon.createMqttConnection(url, clientId, sampleConnection.isTls(),
                    sampleConnection.getCertPath(), conOptions);
            connection.setCallBack(new PubSubCallback());
            topicDevices.put(topic, connection);
        }
    }

    public void startSub() throws Exception {
        for (String topic : topicDevices.keySet()) {
            MqttConnection device = topicDevices.get(topic);
            device.connect();
            PubSubCommon.subscribe(device, topic, 1);
        }
        Thread.sleep(1000);
    }

    public void checkResultMessage(String topic, List<String> expectList) throws Exception {
        if (expectList.size() == 0) {
            MqttConnection device = topicDevices.get(topic);
            Assert.assertFalse("Received msgs are not empty", device.getCallback().checkContain(topic));
        } else {
            List<String> receivedMsgs = topicDevices.get(topic).getCallback()
                    .waitAndGetReveiveListMap(topic, expectList.size());
            Assert.assertEquals(String.format("Received msgs size is wrong, expected %s, but was %s",
                    expectList.toString(), receivedMsgs.toString()), expectList.size(), receivedMsgs.size());

            for (String expectedMsg : expectList) {
                // Check whether it is in json format
                int index = expectList.indexOf(expectedMsg);
                String actualMsg = receivedMsgs.get(index);
                try {
                    Map<String, Object> actualMap = Converter.jsonToModel(actualMsg, Map.class);
                    
                    if (actualMap.containsKey("result")) {
                        log.info("Getting result model from nest msgs"); // TODO ugly implement
                        actualMap = (Map<String, Object>) actualMap.get("result");
                        actualMsg = Converter.modelToJsonUsingJsonNode(actualMap);
                        Map<String, Object> temp = Converter.jsonToModel(expectedMsg, Map.class);
                        temp = (Map<String, Object>) temp.get("result");
                        expectedMsg = Converter.modelToJsonUsingJsonNode(temp);
                    }
                    
                    // Check whether it is in function result
                    try {
                        EdgeDeviceFunctionResultMsg expectedFucntionResult = Converter.jsonToModel(expectedMsg,
                                EdgeDeviceFunctionResultMsg.class);
                        EdgeDeviceFunctionResultMsg actualFucntionResult = Converter.jsonToModel(actualMsg,
                                EdgeDeviceFunctionResultMsg.class);
                        checkFunctionResult(expectedFucntionResult, actualFucntionResult);
                    } catch (Exception e) {
                        // May be error msg
                        if (actualMap.containsKey(ERROR_TYPE)) {
                            Assert.assertEquals("Error type is wrong", expectedMsg, actualMap.get(ERROR_TYPE));
                        } else if (actualMap.containsKey(NOT_NAMED_KEY) && ((Map) actualMap.get(NOT_NAMED_KEY))
                                .containsKey(ERROR_TYPE)) {
                            Assert.assertEquals("Error type is wrong", expectedMsg, ((Map) actualMap.get(
                                    NOT_NAMED_KEY))
                                    .get(ERROR_TYPE));
                        } else {
                            Map<String, Object> expectedMap = Converter.jsonToModel(expectedMsg, Map.class);
                            CheckCommon.checkMapEquals(expectedMap, actualMap);
                        }
                    }
                } catch (Exception e) {
                    Assert.assertEquals("Msg contents are wrong", expectedMsg, actualMsg);
                }
            }
        }
    }

    public void stopChecker() throws Exception {
        for (String topic : topicDevices.keySet()) {
            MqttConnection device = topicDevices.get(topic);
            if (device.getClient().isConnected()) {
                PubSubCommon.unsubscribe(device, topic);
                device.disconnect();
            }
        }
    }

    public void clean() throws Exception {
        stopChecker();
        topicDevices.clear();
    }

    public static void checkFunctionResult(EdgeDeviceFunctionResultMsg expectedResult,
            EdgeDeviceFunctionResultMsg actualResult) throws Exception {
        // Some infos like function invoke id, message id will not be check
        
        // Check context first
        EdgeRuleFunctionContext actualContext = actualResult.getContext();
        EdgeRuleFunctionContext handledActualContext = new EdgeRuleFunctionContext(actualContext.getFunctionName(),
                actualContext.getMessageQOS(), actualContext.getMessageTopic(), actualContext.isMessageRetain());
        CheckCommon.checkMapEquals(Converter.modelToMap(expectedResult.getContext()), Converter
                .modelToMap(handledActualContext));
        
        // Check others
        Map<String, Object> expectedMap = Converter.modelToMap(expectedResult);
        Map<String, Object> actualMap = Converter.modelToMap(actualResult);
        expectedMap.remove("context");
        actualMap.remove("context");
        CheckCommon.checkMapEquals(expectedMap, actualMap);
    }
}
