package edgeTestCases;


import edgeTestCases.utils.client.ConnectionType;
import edgeTestCases.utils.client.EdgeRuleHandlerKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Base class of edge test
 *
 * @author Wu Qi
 */
@SpringApplicationConfiguration(classes = {PropertyPlaceholderAutoConfiguration.class, })
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
public abstract class EDGEIntegrationTest {

    protected Map<String, Integer> offlineEdgePortMap = new HashMap<>();
    protected Map<String, Integer> remoteHubPortMap = new HashMap<>();
    protected TestContextManager testContextManager;
    protected Random random = new Random();
    public static final long SLEEP_TIME = 3000;
    public static final String EMPTY_MAP_STRING = "{}";

    // For localHub
    @Value("${offline.edge.url}")
    protected String offlineEdgeUrl;

    @Value("${offline.edge.tcp.port}")
    protected int offlineEdgeTcpPort;

    @Value("${offline.edge.ssl.port}")
    protected int offlineEdgeSslPort;

    @Value("${offline.edge.ws.port}")
    protected int offlineEdgeWsPort;

    @Value("${offline.edge.wss.port}")
    protected int offlineEdgeWssPort;

    @Value("${offline.edge.username}")
    protected String offlineEdgeUsername;

    @Value("${offline.edge.password}")
    protected String offlineEdgePassword;

    @Value("${offline.edge.another.username}")
    protected String offlineEdgeAnotherUsername;

    @Value("${offline.edge.another.password}")
    protected String offlineEdgeAnotherPassword;

    @Value("${offline.edge.two.way.tls.username}")
    protected String offlineEdgeTwoWayTlsUsername;

    @Value("${edge.single.topic.username}")
    protected String edgeSingleTopicUsername;

    @Value("${edge.single.topic.password}")
    protected String edgeSingleTopicPassword;

    @Value("${edge.permit.check.username}")
    protected String permitCheckUsername;

    @Value("${edge.permit.check.password}")
    protected String permitCheckPassword;

    @Value("${only.plus.username}")
    protected String onlyPlusUsername;

    @Value("${only.plus.password}")
    protected String onlyPlusPassword;

    @Value("${edge.mqtt.msg.byte.limit}")
    protected int payloadLengthLimit;

    @Value("${edge.mqtt.clientid.length.limit}")
    protected int clientIdLengthLimit;

    @Value("${edge.mqtt.topic.length.limit}")
    protected int topicLengthLimit;

    @Value("${edge.cert.path}")
    protected String edgeCertPath;

    @Value("${edge.client.pem.path}")
    protected String edgeClientPemPath;

    @Value("${edge.client.key.path}")
    protected String edgeClientKeyPath;

    @Value("${hub.cert.path}")
    protected String hubCertPath;

    // For function
    @Value("${edge.rule.function.python}")
    protected String pythonFunctionName;

    @Value("${edge.rule.function.python.serial}")
    protected String pythonSerialFunctionName;

    @Value("${edge.rule.function.python.call}")
    protected String pythonCallFunctionName;

    @Value("${edge.rule.function.sql.call}")
    protected String sqlCallFunctionName;

    @Value("${edge.rule.function.python.user}")
    protected String pythonFunctionUser;

    // For remote
    @Value("${edge.remote.mqtt.alltopic.username}")
    protected String remoteUsernameWithAllTopic;

    @Value("${edge.remote.mqtt.alltopic.password}")
    protected String remotePwdWithAllTopic;

    @Value("${edge.remote.mqtt.specific.username}")
    protected String remoteUsernameWithSpecificTopic;

    @Value("${edge.remote.mqtt.specific.password}")
    protected String remotePwdWithSpecificTopic;

    @Value("${edge.remote.mqtt.tcp.port}")
    protected int remoteHubTcpPort;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void init() throws Exception {
        testContextManager = new TestContextManager(getClass());
        testContextManager.prepareTestInstance(this);
        offlineEdgePortMap = initConnectorToPortMap(offlineEdgeTcpPort, offlineEdgeSslPort,
                offlineEdgeWsPort, offlineEdgeWssPort);
        remoteHubPortMap = initConnectorToPortMap(remoteHubTcpPort);
        log.info("-------- Test [{}] Starts -------", testName.getMethodName());
    }

    public static Map<String, Integer> initConnectorToPortMap(int tcpPort) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        result.put(ConnectionType.TCP.toString(), tcpPort);
        return result;
    }

    public static Map<String, Integer> initConnectorToPortMap(int tcpPort, int sslPort, int wsPort, int wssPort) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        result.put(ConnectionType.TCP.toString(), tcpPort);
        result.put(ConnectionType.SSL.toString(), sslPort);
        result.put(ConnectionType.WS.toString(), wsPort);
        result.put(ConnectionType.WSS.toString(), wssPort);

        return result;
    }

    public static void addExpectedMsgToResult(String msg, EdgeRuleHandlerKind kind, Map<EdgeRuleHandlerKind,
            List<String>> resultMap) {
        if (resultMap.containsKey(kind)) {
            resultMap.get(kind).add(msg);
        } else {
            resultMap.put(kind, new ArrayList<String>(Collections.singletonList(msg)));
        }
    }
}
