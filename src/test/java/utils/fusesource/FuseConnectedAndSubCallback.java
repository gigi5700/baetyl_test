package utils.fusesource;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

@Slf4j
/**
 * Simple call with connecting and subscribing specific topic.
 *
 * @author Zhao Meng
 */
public class FuseConnectedAndSubCallback implements Callback {

    protected String clientId;
    protected CallbackConnection connection;
    protected Topic[] topics;
    protected boolean isSubReady = false;
    public static final long TIME_OUT = 15 * 1000;  // 15s
    
    public FuseConnectedAndSubCallback(String clientId, CallbackConnection connection, String topic, QoS qos) {
        Topic[] topics = {new Topic(topic, qos)};
        this.clientId = clientId;
        this.connection = connection;
        this.topics = topics;
    }
    
    @Override
    public void onSuccess(Object value) {
        log.info("{} connected successfully", clientId);
        List<String> topicStrList = new ArrayList<String>();
        for (Topic topic : topics) {
            topicStrList.add(topic.name().ascii().toString());
        }
        connection.subscribe(topics, new Callback<byte[]>() {

            @Override
            public void onSuccess(byte[] value) {
                log.info("{} subscribed {} successfully", clientId, topicStrList.toString());
                isSubReady = true;
            }

            @Override
            public void onFailure(Throwable value) {
                log.error(String.format("%s failed to subscribe %s", clientId, topicStrList), value);
            }
        });
    }

    @Override
    public void onFailure(Throwable value) {
        log.error("Connected successfully", value);

    }

    public boolean waitForSubReady() throws Exception {
        long startTime = System.currentTimeMillis();
        while (!isSubReady && System.currentTimeMillis() - startTime < TIME_OUT) {
            Thread.sleep(2000);
        }
        
        return isSubReady;
    }
}
