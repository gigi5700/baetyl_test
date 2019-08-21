package utils;

import utils.client.ConnectionType;
import utils.client.MqttConnection;
import utils.client.PubSubCallback;
import utils.fusesource.FuseCallbacks;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.Assert;

/**
 * Common functions for pub and sub test.
 *
 * @author Zhao Meng
 */
@Slf4j
public class PubSubCommon {

    private static Random random = new Random();
    private static final String CHARSET = "utf-8";
    private static final String CERT_ALGORITHM = "X.509";
    private static final String TRUST_ALGORITHM = "X509";
    private static final String CERT_TYPE = "TLS";
    private static final String CERT_FILE_TYPE = "ca";

    public static String generateHostUrl(String hostname, ConnectionType connectionType,
             Map<String, Integer> portMap) {
        String connector = connectionType.toString();
        int port = portMap.get(connector);
        return String.format("%s://%s:%s", connector.toLowerCase(), hostname, port);
    }

    public static MqttConnection createMqttConnection(String brokerUrl, String clientId, boolean tls, String certPath,
                                                      MqttConnectOptions connectOptions) throws Exception {
        return new MqttConnection(brokerUrl, clientId, tls, certPath, new MemoryPersistence(),
                connectOptions);
    }

    public static MqttConnection createMqttConnection(String brokerUrl, String clientId, boolean tls, String
            certPath, MqttConnectOptions connectOptions, boolean authCert, String clientCertPath, String clientKeyPath)
            throws Exception {
        return new MqttConnection(brokerUrl, clientId, tls, certPath, new MemoryPersistence(),
                connectOptions, authCert, clientCertPath, clientKeyPath);
    }

    public static MqttConnectOptions getDefaultConnectOptions(String username, String password) {
        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setUserName(username);
        conOpts.setPassword(password.toCharArray());
        conOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);

        return conOpts;
    }

    public static MqttConnectOptions getDefaultConnectOptions(String username) {
        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setUserName(username);
        conOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);

        return conOpts;
    }

    public static MQTT initMqtt(String brokerUrl, String clientId, boolean tls, String certPath,
                                String username, String password) throws Exception {
        MQTT mqtt = new MQTT();

        mqtt.setClientId(clientId);
        mqtt.setHost(brokerUrl);
        mqtt.setUserName(username);
        mqtt.setPassword(password);
        // Default : No retry
        mqtt.setReconnectAttemptsMax(0);

        if (tls) {
            mqtt.setSslContext(genSSLContext(certPath));
        }
        return mqtt;
    }

    public static List<String> publishMessage(MqttConnection pub, String topic, int start, int size, int qos,
                                              boolean retain) throws Exception {
        List<String> pubMessages = new ArrayList<String>();
        for (int i = start; i < size; i++) {
            String message = String.format("%s_%s", topic, i);
            pubMessages.add(message);
            publish(pub, topic, qos, message, retain);
            Thread.sleep(200);
        }
        return pubMessages;
    }

    public static List<String> publishChineseMessage(MqttConnection pub, String topic, int start, int size, int qos,
                                                     boolean retain) throws Exception {
        List<String> pubMessages = new ArrayList<String>();
        for (int i = start; i < size; i++) {
            // Because call back can't handle chinese character
            String message = String.format("chinese_%s", i);
            pubMessages.add(message);
            publish(pub, topic, qos, message, retain);
            Thread.sleep(200);
        }
        return pubMessages;
    }

    public static void checkPubAndSubResult(List<String> pubMessages, List<String> subMessages, int qos) {
        // Check every sub message is contained in pub messages
        for (String subMessage : subMessages) {
            Assert.assertTrue(String.format("Sub messages %s is not in pub messages", subMessage), pubMessages
                    .contains(subMessage));
        }
        // Assert pub size = sub size
        Assert.assertEquals(String.format("Subscribed message amount is not match Published. Expect [%s] But was [%s]",
                pubMessages.toString(), subMessages.toString()), pubMessages.size(), subMessages.size());

        // Check sub message is in order
        Comparator<String> comparator = new Comparator<String>() {

            @Override
            public int compare(String message1, String message2) {
                String[] message1List = message1.split("_");
                String[] message2List = message2.split("_");

                int index1 = Integer.valueOf(message1List[message1List.length - 1]);
                int index2 = Integer.valueOf(message2List[message2List.length - 1]);
                return index1 - index2;
            }
        };
        List<String> temp = new ArrayList<String>();
        temp.addAll(subMessages);
        Collections.sort(temp, comparator);

        StringBuffer order1 = new StringBuffer();
        for (String string : temp) {
            order1.append(string).append(", ");
        }

        StringBuffer order2 = new StringBuffer();
        for (String string : subMessages) {
            order2.append(string).append(", ");
        }

        if (qos > 0) {
            Assert.assertTrue(String.format("Message order is wrong, expect %s, but %s", order1.toString(), order2
                    .toString()), temp.equals(subMessages));
        }
    }

    public static void checkCleanSessionResult(List<String> saveMessages, List<String> pubMessages, List<String>
            subMessages, int qos, boolean cleanSession) {
        // Qos = 0 still can receive clean session results
        if (cleanSession) {
            checkPubAndSubResult(pubMessages, subMessages, qos);
        } else {
            List<String> messages = new ArrayList<String>();
            messages.addAll(saveMessages);
            messages.addAll(pubMessages);
            checkPubAndSubResult(messages, subMessages, qos);
        }
    }

    public static void checkRetainResult(List<String> pubMessages, List<String> subMessages, int qos,
                                         boolean retained) {
        if (retained) {
            Assert.assertTrue(String.format("Size is not 1 %s", subMessages), subMessages.size() == 1);
            String expectedContent = pubMessages.get(pubMessages.size() - 1);
            String actualContent = subMessages.get(0);
            Assert.assertTrue(String.format("Content is wrong. Expected %s, but %s", expectedContent, actualContent),
                    actualContent.equalsIgnoreCase(expectedContent));
        } else {
            Assert.assertTrue("Size is not 0", subMessages.size() == 0);
        }
    }

    public static void checkRetainResult(List<String> pubMessages, PubSubCallback callback, int qos, boolean retain,
                                         boolean checkRetainFlag) throws Exception {
        long timeout = retain ? PubSubCallback.WAIT_TIME_OUT : 2L;
        checkRetainResult(pubMessages, callback.waitAndGetReveiveList(1, timeout), qos, retain);
        if (checkRetainFlag && retain) {
            log.info("Checking retain flag should be {}", retain);
            boolean retainFlag = callback.getRawMqttMsgReceiveList().get(0).isRetained();
            Assert.assertEquals("Retain flag is wrong", retain, retainFlag);
        }
    }
    
    public static void checkWillResult(String willMessage, List<String> subMessages, int qos) {
        if (StringUtils.isEmpty(willMessage)) {
            Assert.assertEquals("Should receive no will message", 0, subMessages.size());
        } else {
            Assert.assertTrue(String.format("Size is not 1, but %s", subMessages.size()), subMessages.size() == 1);
            Assert.assertTrue("will message doesn't work", subMessages.contains(willMessage));
        }
    }
    
    public static void checkWillResult(String willMessage, PubSubCallback callback, int qos, boolean retain)
            throws Exception {
        List<String> subMessages = callback.waitAndGetReveiveList();
        checkWillResult(willMessage, subMessages, qos);
        if (!StringUtils.isEmpty(willMessage)) {
            log.info("Checking retain flag should be {}", retain);
            boolean retainFlag = callback.getRawMqttMsgReceiveList().get(0).isRetained();
            Assert.assertEquals("Retain flag is wrong", retain, retainFlag);
        }
    }

    public static void subscribe(MqttConnection connection, String topic, int qos) throws Exception {
        subscribe(connection, new String[]{topic}, new int[]{qos});
    }

    public static IMqttToken subscribe(MqttConnection connection, List<String> topics, int qos) throws Exception {
        String[] topicArray = new String[topics.size()];
        for (int i = 0; i < topicArray.length; i++) {
            topicArray[i] = topics.get(i);
        }
        int[] qosArray = new int[topicArray.length];
        for (int i = 0; i < qosArray.length; i++) {
            qosArray[i] = qos;
        }
        return subscribe(connection, topicArray, qosArray);
    }

    public static IMqttToken subscribe(MqttConnection connection, String[] topics, int[] qosList) throws Exception {

        if (null == connection) {
            return null;
        }

        MqttAsyncClient client = connection.getClient();
        if (client == null || !client.isConnected()) {
            log.info("Sub reconnect");
            connection.connect();
        }

        try {
            IMqttToken token = client.subscribe(topics, qosList);
            token.waitForCompletion(MqttConnection.ACTION_TIME_OUT);
            log.info("Client {} subscribed topics {} get {}", connection.getClient().getClientId(),
                    Arrays.toString(topics), token.getResponse().toString());
            return token;
        } catch (Exception e) {
            log.error("Sub failed", e);
            return null;
        }
    }

    public static IMqttToken subscribeWithReturn(MqttConnection connection, String topic, int qos) throws Exception {
        if (null == connection) {
            return null;
        }

        MqttAsyncClient client = connection.getClient();
        if (client == null || !client.isConnected()) {
            log.info("Sub reconnect");
            connection.connect();
        }

        IMqttToken token = connection.getClient().subscribe(topic, qos);
        token.waitForCompletion(MqttConnection.ACTION_TIME_OUT);
        log.info("Subscribe topic {} got {}", topic, token.getResponse().toString());
        return token;
    }

    public static void publish(MqttConnection connection, String topic, int qos, String message, boolean retained)
            throws Exception {

        if (null == connection) {
            return;
        }

        MqttAsyncClient client = connection.getClient();
        if (client == null || !client.isConnected()) {
            log.info("Pub reconnect");
            connection.connect();
        }

        try {
            log.info("Client {} start to publish {} to topic {}", client.getClientId(), message, topic);
            client.publish(topic, message.getBytes(CHARSET), qos, retained).waitForCompletion(MqttConnection
                    .ACTION_TIME_OUT);
            log.info("Client {} published {} to topic {} successfully", client.getClientId(), message, topic);
        } catch (Exception e) {
            log.error(String.format("Pub %s published %s failed", client.getClientId(), message), e);
        }
    }

    public static void publish(final CallbackConnection connection, final String topic, final QoS qos,
                               final String message, final boolean retained) throws Exception {

        if (null == connection) {
            log.error("Connection is null");
            return;
        }

        connection.getDispatchQueue().execute(new Runnable() {

            @Override
            public void run() {
                connection.publish(topic, message.getBytes(), qos, retained, new Callback<Void>() {

                    @Override
                    public void onSuccess(Void value) {
                        log.info("Pub {} successfully", message);
                    }

                    @Override
                    public void onFailure(Throwable value) {
                        log.error(String.format("Pub %s failed", message), value);
                    }
                });
            }
        });
    }

    public static void unsubscribe(MqttConnection connection, String topic) throws Exception {

        if (null == connection) {
            return;
        }

        MqttAsyncClient client = connection.getClient();
        if (client == null || !client.isConnected()) {
            System.out.println("Connection has already lost");
            return;
        }

        client.unsubscribe(topic).waitForCompletion(MqttConnection.ACTION_TIME_OUT);
    }

    public static void disconnectCallbackConnection(final CallbackConnection connection) {
        if (null == connection) {
            log.error("Connection is null");
            return;
        }

        connection.getDispatchQueue().execute(new Runnable() {

            @Override
            public void run() {
                connection.disconnect(new FuseCallbacks("disconnect"));
            }
        });
    }

    public static void stopConnectionForciblyWithoutWait(MqttConnection connection) throws Exception {

        if (null == connection) {
            return;
        }
        connection.getClient().disconnectForcibly(MqttConnection.DISCON_TIME_OUT, 0, false);
    }

    public static SSLContext genSSLContext(String certPath) throws Exception {
        File file = new File(certPath);
        InputStream certInputStream = new FileInputStream(file);

        CertificateFactory certFactory = CertificateFactory.getInstance(CERT_ALGORITHM);
        Certificate cert = certFactory.generateCertificate(certInputStream);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        keyStore.setCertificateEntry(CERT_FILE_TYPE, cert);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TRUST_ALGORITHM);
        trustManagerFactory.init(keyStore);
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

        SSLContext context = SSLContext.getInstance(CERT_TYPE);
        context.init(null, trustManagers, null);

        return context;
    }
}