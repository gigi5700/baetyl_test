package baetylTest.function;

import baetylTest.EDGEIntegrationTest;
import baetylTest.utils.Converter;
import baetylTest.utils.PubSubCommon;
import baetylTest.utils.client.ConnectionType;
import baetylTest.utils.client.EdgeDeviceMQTTChecker;
import baetylTest.utils.client.EdgeRuleFunctionContext;
import baetylTest.utils.client.MqttConnection;
import baetylTest.utils.msg.EdgeDeviceFunctionCallMsg;
import baetylTest.utils.msg.EdgeDeviceFunctionResultMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Test cases for function handler of subscriptions.
 *
 * @author Zhao Meng
 */
@Slf4j
public class EdgeDeviceReFunctionTest extends EDGEIntegrationTest {

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
        qos = 0;
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
     * TestGoal: Test python function handler.
     *
     * Main steps:
     *  Step1: Connect a pub client and a sub client. Sub client subscribes the target topic.
     *  Step2: Publish one msg having return value of function and one msg not.
     *  Step3: Check target topic will only receive one msg and the context about mqtt features in msg should
     *   be correct.
     *
     * @throws Exception
     */
    @Test
    public void testPythonFunction() throws Exception {
        // Having return value or not, both check context
        String sourceTopic = "python/function/test";
        String targetTopic = "python/function/result";
        checker.bindTopic(targetTopic);
        checker.startSub();
        
        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("%s_%s", testName.getMethodName(), System.currentTimeMillis()), tls, edgeCertPath,
                connectOptions);
        
        EdgeDeviceFunctionCallMsg returnValueMsg = new EdgeDeviceFunctionCallMsg();
        returnValueMsg.setValue("returnValue" + System.currentTimeMillis());
        
        EdgeDeviceFunctionCallMsg noReturnValueMsg = new EdgeDeviceFunctionCallMsg();
        noReturnValueMsg.setNoReturnValue("noreturn");
        
        EdgeRuleFunctionContext context = new EdgeRuleFunctionContext(pythonFunctionName, qos, sourceTopic, false);
        EdgeDeviceFunctionResultMsg resultMsg = new EdgeDeviceFunctionResultMsg(pythonFunctionUser, context);
        resultMsg.setValue(returnValueMsg.getValue());
        
        PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(returnValueMsg), false);
        Thread.sleep(200);
        PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(noReturnValueMsg), false);
        checker.checkResultMessage(targetTopic, Collections.singletonList(Converter
                .modelToJsonUsingJsonNode(resultMsg)));
        pub.disconnect();
    }


    /**
     * TestGoal: Test msg order of function handler.
     *
     * Main steps: Pub a few correct msgs with qos=0 to function with multi instances, check the order of results.
     *
     * @throws Exception
     */
    @Test
    public void testFunctionMsgOrder() throws Exception {
        qos = 1;
        String sourceTopic = "python/function/test";
        String targetTopic = "python/function/result";
        checker.bindTopic(targetTopic);
        checker.startSub();
        EdgeRuleFunctionContext context = new EdgeRuleFunctionContext(pythonFunctionName, qos, sourceTopic, false);
        EdgeDeviceFunctionResultMsg resultMsg = new EdgeDeviceFunctionResultMsg(pythonFunctionUser, context);

        MqttConnection pub = PubSubCommon.createMqttConnection(
                PubSubCommon.generateHostUrl(offlineEdgeUrl, connectionType, offlineEdgePortMap),
                String.format("%s_%s", testName.getMethodName(), System.currentTimeMillis()), tls, edgeCertPath,
                connectOptions);

        int msgsCount = 20 + random.nextInt(10);
        log.info("Publish {} msgs", msgsCount);
        List<EdgeDeviceFunctionCallMsg> pubMsgs = new ArrayList<EdgeDeviceFunctionCallMsg>();
        List<String> resultMsgs = new ArrayList<String>();
        for (int i = 0; i < msgsCount; i++) {
            EdgeDeviceFunctionCallMsg msg = new EdgeDeviceFunctionCallMsg();
            msg.setValue("returnValue" + i);
            pubMsgs.add(msg);
            
            resultMsg.setValue(msg.getValue());
            resultMsgs.add(Converter.modelToJsonUsingJsonNode(resultMsg));
        }
        
        for (EdgeDeviceFunctionCallMsg msg : pubMsgs) {
            PubSubCommon.publish(pub, sourceTopic, qos, Converter.modelToJsonUsingJsonNode(msg), false);
            Thread.sleep(100);
        }
        
        checker.checkResultMessage(targetTopic, resultMsgs);
        pub.disconnect();
    }
}
