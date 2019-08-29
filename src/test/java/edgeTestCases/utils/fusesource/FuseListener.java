package edgeTestCases.utils.fusesource;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.MQTT;

@Slf4j
@Data
/**
 * Simple listener for fusesource edgeTestCases.mqtt async client.
 *
 * @author Zhao Meng
 */
public class FuseListener implements Listener {
    
    protected MQTT mqtt = new MQTT();
    protected String clientId = "";
    protected boolean isConnected = false;
    protected List<String> receiveMsg = new ArrayList<String>();
    public static final long TIME_OUT = 20 * 1000;  // 20s

    public FuseListener() {
    }

    @Override
    public void onConnected() {
        isConnected = true;
        log.info("Connection {} connected {}", clientId, mqtt.getHost().toString());
    }

    @Override
    public void onDisconnected() {
        isConnected = false;
        log.info("Connection {} disconnected from {}", clientId, mqtt.getHost().toString());
    }

    @Override
    public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
        ack.run();
        String msg = body.ascii().toString();
        receiveMsg.add(msg);
        log.info("Client {} receive msg {}", clientId, msg);
    }

    @Override
    public void onFailure(Throwable value) {
        isConnected = false;
        log.error(String.format("Error occurred in %s listener", clientId), value);
    }
}
