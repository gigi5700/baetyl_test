package test;

import endpoint.EndpointManager;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import util.client.ConnectionType;

/**
 * Base class of edge test
 *
 * @author Wu Qi(wuqi06@baidu.com)
 */
@SpringApplicationConfiguration(classes = {PropertyPlaceholderAutoConfiguration.class, })
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
public class EDGEIntegrationTest {
    protected Map<String, Integer> offlineEdgePortMap = new HashMap<>();
    protected TestContextManager testContextManager;
    protected Random random = new Random();
    public static final long SLEEP_TIME = 3000;

    // For localHub
    @Value("${offline.edge.url}")
    protected String offlineEdgeUrl;

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

    @Value("${offline.edge.tcp.port}")
    protected int offlineEdgeTcpPort;

    @Value("${offline.edge.ssl.port}")
    protected int offlineEdgeSslPort;

    @Value("${offline.edge.ws.port}")
    protected int offlineEdgeWsPort;

    @Value("${offline.edge.wss.port}")
    protected int offlineEdgeWssPort;

    @Value("${edge.cert.path}")
    protected String edgeCertPath;

    @Value("${hub.cert.path}")
    protected String hubCertPath;

    @Value("${edge.client.pem.path}")
    protected String edgeClientPemPath;

    @Value("${edge.client.key.path}")
    protected String edgeClientKeyPath;

    @Value("${mqtt.msg.byte.limit}")
    protected int payloadLengthLimit;

    @Value("${mqtt.clientid.length.limit}")
    protected int clientIdLengthLimit;

    @Value("${mqtt.topic.length.limit}")
    protected int topicLengthLimit;

    @Rule
    public TestName testName = new TestName();

    static {
        try (InputStream endpointInputStream = EDGEIntegrationTest.class.getResourceAsStream(
                "/endpoint.json")) {
            String endpointConfig = IOUtils.toString(endpointInputStream, StandardCharsets.UTF_8);
            EndpointManager.setInstance(EndpointManager.createFromConfig(endpointConfig));

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Before
    public void init() throws Exception {
        testContextManager = new TestContextManager(getClass());
        testContextManager.prepareTestInstance(this);
        offlineEdgePortMap = initConnectorToPortMap(offlineEdgeTcpPort, offlineEdgeSslPort,
                offlineEdgeWsPort, offlineEdgeWssPort);
        log.info("-------- Test [{}] Starts -------", testName.getMethodName());
    }

    public static Map<String, Integer> initConnectorToPortMap(int tcpPort, int sslPort, int wsPort, int wssPort) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        result.put(ConnectionType.TCP.toString(), tcpPort);
        result.put(ConnectionType.SSL.toString(), sslPort);
        result.put(ConnectionType.WS.toString(), wsPort);
        result.put(ConnectionType.WSS.toString(), wssPort);

        return result;
    }
}