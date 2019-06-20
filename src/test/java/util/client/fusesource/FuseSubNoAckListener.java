package util.client.fusesource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Assert;

/**
 * Listener for abnormal pub sub scenario.
 * It will sleep when receiving msg until isPending is false. 
 *
 * @author Zhao Meng (zhaomeng04@baidu.com)
 */
@Data
@Slf4j
public class FuseSubNoAckListener extends FuseListener {

    protected boolean sendAck = false;
    protected Map<String, List<String>> receiveListMap = new HashMap<String, List<String>>();
    protected List<Buffer> receivedBufferMsgList = new ArrayList<Buffer>();
    public static final long WAIT_TIME_OUT = 10 * 1000;
    public static final long REFORWARD_TIME = 10 * 1000;     // 10s

    public FuseSubNoAckListener(MQTT mqtt) {
        this.mqtt = mqtt;
        this.clientId = mqtt.getClientId().toString();
    }
    
    @Override
    public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
        String topicStr = topic.toString();
        String payload = body.ascii().toString();
        receivedBufferMsgList.add(body);
        synchronized (this) {
            log.info("Client {} receive msg {} from topic {}", clientId, payload, topicStr);
            if (receiveListMap.containsKey(topicStr)) {
                receiveListMap.get(topicStr).add(payload);
            } else {
                receiveListMap.put(topicStr, new ArrayList<String>(Collections.singletonList(payload)));
            }
        }
        if (!sendAck) {
            log.info("Do not send ack to msg {}", payload);
        } else {
            ack.run();
            log.info("Send ack to msg {}", payload);
        }
    }
    
    public List<Buffer> getRawReceivedBufferMsgList() {
        List<Buffer> result = new ArrayList<Buffer>();
        result.addAll(receivedBufferMsgList);
        synchronized (this) {
            receivedBufferMsgList.clear();
        }
        return result;
    }
    
    public Map<String, List<String>> getRawReceiveListMap() {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        result.putAll(receiveListMap);
        synchronized (this) {
            receiveListMap.clear();
        }
        return result;
    }
    
    public List<String> getReceiveListMap(String topic) {
        List<String> result = new ArrayList<String>();
        if (receiveListMap.containsKey(topic)) {
            result.addAll(receiveListMap.get(topic));
            synchronized (this) {
                receiveListMap.get(topic).clear();
            }
        }
        return result;
    }
    
    public List<String> waitAndGetReveiveListMap(String topic, int expectedCount, long... timeout)
            throws Exception {
        long waitTimeOut = WAIT_TIME_OUT;
        if (timeout.length > 0) {
            waitTimeOut = timeout[0] * 1000;
        }
        
        long start = System.currentTimeMillis();
        while ((!receiveListMap.containsKey(topic) || receiveListMap.get(topic).size() < expectedCount) && System
                .currentTimeMillis() - start < waitTimeOut) {
            Thread.sleep(1000);
            log.info("Waiting for msg for topic {} ...", topic);
        }
        return getReceiveListMap(topic);
    }
    
    public static void checkDupFlag(Buffer msg, boolean dup) {
        log.info("Check dup flag should be {}", dup);
        int dupFlag = msg.data[0] >> 3 & 1;
        Assert.assertEquals("Dup flag is wrong", dup, dupFlag == 1 ? true : false);
    }
}
