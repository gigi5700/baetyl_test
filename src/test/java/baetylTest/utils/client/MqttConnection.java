package baetylTest.utils.client;

import sun.misc.BASE64Decoder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Connection for mqtt using paho MqttClient.
 *
 * @author Zhao Meng
 */
@Slf4j
@Data
public class MqttConnection {

    private MqttAsyncClient client;
    private boolean tls;
    private String certPath;
    private MqttConnectOptions connOpts;
    private PubSubCallback callback;
    private boolean authCert = false;
    private String clientCertPath;
    private String clientKeyPath;

    public static long minLatency = 0;
    public static long maxLatency = 0;
    public static double meanLatency = 0;

    public static final int CON_TIME_OUT = 10000;
    public static final int DISCON_TIME_OUT = 30000;
    public static final int KEEP_ALIVE_TIME = 60;
    public static final int ACTION_TIME_OUT = 30000;

    public static long maxConnection = 0;
    public static long connectionCount = 0;
    public static long connectLost = 0;

    public MqttConnection(String broker, String clientId, boolean tls, String certPath, MemoryPersistence persistence,
                          MqttConnectOptions connOpts) {
        super();
        this.tls = tls;
        this.certPath = certPath;
        this.connOpts = connOpts;
        try {
            this.client = new MqttAsyncClient(broker, clientId, persistence);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public MqttConnection(String broker, String clientId, boolean tls, String certPath, MemoryPersistence persistence,
                          MqttConnectOptions connOpts, boolean authCert, String clientCertPath, String clientKeyPath) {
        super();
        this.tls = tls;
        this.certPath = certPath;
        this.connOpts = connOpts;
        this.authCert = authCert;
        this.clientCertPath = clientCertPath;
        this.clientKeyPath = clientKeyPath;
        try {
            this.client = new MqttAsyncClient(broker, clientId, persistence);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public boolean connect() throws Exception {
        boolean isConnect = false;
        if (connOpts == null) {
            connOpts = getDefaultConnectOptions();
        }

        if (authCert) {
            if (tls) {
                connOpts.setSocketFactory(getSSLTLSSocketFactory(certPath, clientCertPath,
                        clientKeyPath));
            } else {
                connOpts.setSocketFactory(getSSLSocketFactory(clientCertPath, clientKeyPath));
            }
        } else {
            if (tls) {
                connOpts.setSocketFactory(getTLSSocketFactory(certPath));
            } else {
                connOpts.setSocketFactory(null);
            }
        }

        long startTime = System.currentTimeMillis();
        try {
            log.info("Connecting.. clientId {}, broker {}, wait timeout {}", client.getClientId(), client
                    .getServerURI(), connOpts.getConnectionTimeout() * 1000);
            client.connect(connOpts).waitForCompletion(connOpts.getConnectionTimeout() * 1000);
            long endTime = System.currentTimeMillis();

            synchronized (MqttConnection.class) {
                maxConnection++;
                connectionCount++;
                updateLantency(endTime - startTime);
                isConnect = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            disconnect();
            log.error("Connect {} failed : {}", client.getClientId(), e.getMessage());
            throw e;
        }
        return isConnect;
    }

    public void setCallBack(PubSubCallback callback) {
        this.callback = callback;
        client.setCallback(callback);
    }

    private SSLSocketFactory getSSLTLSSocketFactory(String certPath, String clientCertPath, String clientKeyPath)
            throws Exception {
        TrustManager[] trustManagers = getTrustManager(certPath);

        KeyManager[] km = getKeyManager(clientCertPath, clientKeyPath);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(km, trustManagers, null);
        return context.getSocketFactory();
    }

    private SSLSocketFactory getSSLSocketFactory(String clientCertPath, String clientKeyPath)
            throws Exception {
        KeyManager[] km = getKeyManager(clientCertPath, clientKeyPath);
        SSLContext context = SSLContext.getInstance("SSL");
        context.init(km, null, null);
        return context.getSocketFactory();
    }


    private SocketFactory getTLSSocketFactory(String certPath) throws Exception {
        TrustManager[] trustManagers = getTrustManager(certPath);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers, null);
        return context.getSocketFactory();
    }

    private TrustManager[] getTrustManager(String certPath)
            throws Exception {
        File file = new File(certPath);
        InputStream certInputStream = new FileInputStream(file);
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        Certificate cert = certFactory.generateCertificate(certInputStream);
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        keyStore.setCertificateEntry("ca", cert);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        trustManagerFactory.init(keyStore);
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        return trustManagers;
    }

    private KeyManager[] getKeyManager(String clientCertPath, String clientKeyPath)
            throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        byte[] certBytes = fileToBytes(clientCertPath);
        PEMReader reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(certBytes)));
        X509Certificate x509Certificate = (X509Certificate) reader.readObject();
        PrivateKey privateKey = getPrivateKey(clientKeyPath, "RSA");
        KeyStore keyStore1 = KeyStore.getInstance("JCEKS");
        keyStore1.load(null);
        keyStore1.setCertificateEntry("cert-alias", x509Certificate);
        keyStore1.setKeyEntry("key-alias", privateKey, "changeit".toCharArray(), new Certificate[]{x509Certificate});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore1, "changeit".toCharArray());
        KeyManager[] km = kmf.getKeyManagers();
        return km;
    }


    public boolean disconnect() {
        if (client != null && client.isConnected()) {
            try {
                // Wait until isConnected=false, because paho marks complete as soon as disconnect is sent
                long startTime = System.currentTimeMillis();
                log.info("Disconnecting client {}", client.getClientId());
                client.disconnect().waitForCompletion(DISCON_TIME_OUT);
                while (client.isConnected() && System.currentTimeMillis() - startTime <= DISCON_TIME_OUT) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (client.isConnected()) {
                    log.error("Connection doesn't disconnect in time out");
                    return false;
                }

                return true;
            } catch (MqttException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private MqttConnectOptions getDefaultConnectOptions() {
        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setCleanSession(true);
        conOpts.setUserName("admin");
        conOpts.setPassword("password".toCharArray());
        conOpts.setConnectionTimeout(CON_TIME_OUT);
        conOpts.setKeepAliveInterval(KEEP_ALIVE_TIME);

        return conOpts;
    }

    public static void updateLantency(long latency) {
        if (connectionCount > 0) {
            meanLatency = (double) (meanLatency * (maxConnection - 1) + latency) / (double) maxConnection;
            if (latency < minLatency) {
                minLatency = latency;
            }

            if (latency > maxLatency) {
                maxLatency = latency;
            }
        } else {
            log.warn("Connection count is <= 0");
        }
    }

    public static PrivateKey getPrivateKey(String filename, String algorithm) throws Exception {
        File f = new File(filename);
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f.length()];
        dis.readFully(keyBytes);
        dis.close();
        String temp = new String(keyBytes);
        String privKeyPEM = temp.replace("-----BEGIN RSA PRIVATE KEY-----", "");
        privKeyPEM = privKeyPEM.replace("-----END RSA PRIVATE KEY-----", "");
        BASE64Decoder b64 = new BASE64Decoder();
        byte[] decoded = b64.decodeBuffer(privKeyPEM);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decoded);
        KeyFactory kf = KeyFactory.getInstance(algorithm);
        return kf.generatePrivate(spec);
    }

    public static byte[] fileToBytes(String path) throws IOException {
        File file = new File(path);
        InputStream input = new FileInputStream(file);
        byte[] bytes = new byte[input.available()];
        input.read(bytes);
        return bytes;
    }
}
