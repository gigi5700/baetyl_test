package baetylTest.mqtt;

import baetylTest.EDGEIntegrationTest;
import baetylTest.utils.CheckCommon;
import baetylTest.utils.PubSubCommon;
import baetylTest.utils.client.ConnectionType;
import baetylTest.utils.client.MqttConnection;
import baetylTest.utils.client.PubSubCallback;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test cases for protocol connection of mqtt device
 *
 * @author Zhao Meng
 */
@RunWith(Parameterized.class)
public class EdgeDeviceMqttConnectionProtocolTest extends EDGEIntegrationTest {

    public ConnectionType connectionType;
    public boolean tls;
    public static MqttConnectOptions connectOptions;
    public static int qos;

    public EdgeDeviceMqttConnectionProtocolTest(ConnectionType connectionType, boolean tls) {
        super();
        this.connectionType = connectionType;
        this.tls = tls;
    }

    @SuppressWarnings("rawtypes")
    @Parameters(name = "connection: [{0}]")
    public static Collection provideData() {
        
        Object[][] data = new Object[][]{
                {ConnectionType.TCP, false},
                {ConnectionType.SSL, true},
                {ConnectionType.WS, false},
                {ConnectionType.WSS, true}
        };
        return Arrays.asList(data);
    }
    
    @Before
    public void setUp() throws Exception {
        qos = random.nextInt(2);
        connectOptions = PubSubCommon.getDefaultConnectOptions(offlineEdgeUsername, offlineEdgePassword);
    }
    
    /**
     * TestGoal: Test connecting mqtt with different supported protocols
     *
     * @throws Exception
     */
    @Test
    public void testConnectionWithDifferentProtocol() throws Exception {
        String topic = String.format("%s/test", connectionType.toString());
        String clientId = String.format("%s_connect_%s", connectionType.toString(), System.currentTimeMillis());
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, tls, edgeCertPath, connectOptions);
        connection.setCallBack(new PubSubCallback());
        try {
            connection.connect();
            PubSubCommon.subscribe(connection, topic, random.nextInt(2));
            List<String> pubMsgs = PubSubCommon.publishMessage(connection, topic, 0, 5, qos, false);
            PubSubCommon.checkPubAndSubResult(pubMsgs, connection.getCallback().waitAndGetReveiveList(pubMsgs
                    .size()), qos);
        } catch (Exception e) {
            throw e;
        } finally {
            connection.disconnect();
        }
    }
    
    /**
     * TestGoal: Test connecting mqtt with missing cert in ssl context
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithSslButNoCert() throws Exception {
        Assume.assumeTrue(ConnectionType.SSL.equals(connectionType) || ConnectionType.WSS.equals(connectionType));
        String clientId = String.format("%s_connectWithoutCert_%s", connectionType.toString(), System
                .currentTimeMillis());
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, false, null, connectOptions);
        CheckCommon.checkConnectionFail(connection);
    }
    
    /**
     * TestGoal: Test connecting mqtt with wrong cert in ssl context
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithSslButWrongCert() throws Exception {
        Assume.assumeTrue(ConnectionType.SSL.equals(connectionType) || ConnectionType.WSS.equals(connectionType));
        String clientId = String.format("%s_connectWithoutCert_%s", connectionType.toString(), System
                .currentTimeMillis());
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, tls, hubCertPath, connectOptions);
        CheckCommon.checkConnectionFail(connection);
    }
    
    /**
     * TestGoal: Test connecting mqtt with a cert in non-ssl context
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithoutSslButHavingCertInContext() throws Exception {
        Assume.assumeTrue(ConnectionType.TCP.equals(connectionType) || ConnectionType.WS.equals(connectionType));
        String clientId = String.format("%s_notssl_%s", connectionType.toString(), System
                .currentTimeMillis());

        // One way tls
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, connectOptions);
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_SOCKET_FACTORY_MISMATCH);

        // Two way tls
        MqttConnectOptions conOpts = PubSubCommon.getDefaultConnectOptions(offlineEdgeTwoWayTlsUsername);
        connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, conOpts, true,
                edgeClientPemPath, edgeClientKeyPath);
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_SOCKET_FACTORY_MISMATCH);
    }
    
    /**
     * TestGoal: Test connecting mqtt with illegal protocols
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithIllegalProtocol() throws Exception {
        // Run once
        Assume.assumeTrue(ConnectionType.TCP.equals(connectionType));
        String clientId = String.format("%s_wrongproctol_%s", connectionType.toString(), System
                .currentTimeMillis());

        // Wrong port
        String wrongUrl = String.format("%s://%s:%s", connectionType.toString().toLowerCase(), offlineEdgeUrl,
                offlineEdgePortMap.get(ConnectionType.SSL.toString()));
        MqttConnection connection = PubSubCommon.createMqttConnection(wrongUrl, clientId, false, edgeCertPath,
                connectOptions);
        CheckCommon.checkConnectionFail(connection);
    }
    
    /**
     * TestGoal: Test connecting mqtt twice with the same client
     *
     * @throws Exception
     */
    @Test
    public void testConnectTwice() throws Exception {
        String clientId = String.format("%s_connectTwice_%s", connectionType.toString(), System
                .currentTimeMillis());
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, tls, edgeCertPath, connectOptions);
        connection.connect();
        CheckCommon.checkConnectionFail(connection, MqttException.REASON_CODE_CLIENT_CONNECTED);
        connection.disconnect();
    }

    /**
     * TestGoal: Test connecting mqtt with two-way tls mode and check msg re-forward and illegal scenarios.
     *
     * @throws Exception
     */
    @Test
    public void testConnectWithTwoWayTLS() throws Exception {
        Assume.assumeTrue(ConnectionType.SSL.equals(connectionType) || ConnectionType.WSS.equals(connectionType));
        // Connect with two way tls
        MqttConnectOptions conOpts = PubSubCommon.getDefaultConnectOptions(offlineEdgeTwoWayTlsUsername);
        String clientId = "twoway_tls" + System.currentTimeMillis();
        MqttConnection connection = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, conOpts, true,
                        edgeClientPemPath, edgeClientKeyPath);
        connection.setCallBack(new PubSubCallback());
        connection.connect();

        // Pub and sub msgs
        String topic = "two/way" + System.currentTimeMillis();
        PubSubCommon.subscribe(connection, topic, qos);

        List<String> pubMsgs = PubSubCommon.publishMessage(connection, topic, 0, 5, qos, false);
        PubSubCommon.checkPubAndSubResult(pubMsgs, connection.getCallback().waitAndGetReveiveList(pubMsgs.size()), qos);
        connection.disconnect();

        // Connect with no cert
        MqttConnection conWithNoCert = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, conOpts, true,
                "", "");
        CheckCommon.checkConnectionFail(conWithNoCert);

        // Connect with wrong cert
        MqttConnection wrongCert = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, conOpts, true,
                edgeClientPemPath, hubCertPath);
        CheckCommon.checkConnectionFail(wrongCert);

        // Connect with no username
        MqttConnection noUsername = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, new MqttConnectOptions(),
                        true, edgeClientPemPath, edgeClientKeyPath);
        CheckCommon.checkConnectionFail(noUsername);

        // Connect with wrong username
        conOpts.setUserName(offlineEdgeUsername);
        MqttConnection wrongUsername = PubSubCommon.createMqttConnection(PubSubCommon.generateHostUrl(offlineEdgeUrl,
                connectionType, offlineEdgePortMap), clientId, true, edgeCertPath, conOpts,
                true, edgeClientPemPath, edgeClientKeyPath);
        CheckCommon.checkConnectionFail(noUsername);
    }
}