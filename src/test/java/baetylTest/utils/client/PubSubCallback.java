package baetylTest.utils.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@Slf4j
public class PubSubCallback implements MqttCallback {
    private List<String> receiveList = new ArrayList<String>();
    private List<MqttMessage> rawMqttMsgReceiveList = new ArrayList<MqttMessage>();
    private Map<String, String> lastMsgMap = new HashMap<String, String>();
    private Map<String, List<String>> receiveListMap = new HashMap<String, List<String>>();
    private long lastReceiveMessageTime = 0;
    private List<Long> lastReceiveMessageTimeList = new ArrayList<Long>();
    private static final String CHARSET = "utf-8";
    public static final long WAIT_TIME_OUT = 10 * 1000;
    public AtomicInteger conLostCount = new AtomicInteger();

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        synchronized (this) {
            rawMqttMsgReceiveList.add(message);
            lastReceiveMessageTime = System.currentTimeMillis();
            lastReceiveMessageTimeList.add(lastReceiveMessageTime);
            String payload = new String(message.getPayload(), CHARSET);
            log.info("Received msg {} from topic {}", payload, topic);
            receiveList.add(payload);
            lastMsgMap.put(topic, payload);
            if (receiveListMap.containsKey(topic)) {
                receiveListMap.get(topic).add(payload);
            } else {
                receiveListMap.put(topic, new ArrayList<String>(Collections.singletonList(payload)));
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("connection lost", cause);
        conLostCount.incrementAndGet();
    }

    public List<String> getReceiveList() {
        List<String> result = new ArrayList<String>();
        result.addAll(receiveList);

        synchronized (this) {
            receiveList.clear();
        }
        return result;
    }

    public List<MqttMessage> getRawMqttMsgReceiveList() {
        List<MqttMessage> result = new ArrayList<MqttMessage>();
        result.addAll(rawMqttMsgReceiveList);

        synchronized (this) {
            rawMqttMsgReceiveList.clear();
        }
        return result;
    }

    public Map<String, String> getLastMsgMap() {
        Map<String, String> result = new HashMap<String, String>();
        result.putAll(lastMsgMap);
        synchronized (this) {
            lastMsgMap.clear();
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

    public Map<String, List<String>> getReceiveListMap() {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        result.putAll(receiveListMap);
        synchronized (this) {
            receiveListMap.clear();
        }
        return result;
    }

    public List<Long> getLastReceiveTimeList() {
        List<Long> result = new ArrayList<Long>();
        result.addAll(lastReceiveMessageTimeList);

        synchronized (this) {
            lastReceiveMessageTimeList.clear();
        }
        return result;
    }

    public List<String> waitAndGetReveiveList(long... timeout) throws Exception {
        return waitAndGetReveiveList(1, timeout);
    }

    public List<String> waitAndGetReveiveList(int expectedCount, long... timeout) throws Exception {
        long waitTimeOut = WAIT_TIME_OUT;
        if (timeout.length > 0) {
            waitTimeOut = timeout[0] * 1000;
        }

        long start = System.currentTimeMillis();
        while (receiveList.size() < expectedCount && System.currentTimeMillis() - start < waitTimeOut) {
            Thread.sleep(1000);
            log.info("Waiting for msg...");
        }
        return getReceiveList();
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

    public boolean checkContain(String topic) {
        boolean contain = false;
        if (!receiveListMap.isEmpty() && receiveListMap.containsKey(topic) && !receiveListMap.get(topic).isEmpty()) {
            contain = true;
        }
        return contain;
    }

    public void clear() {
        getReceiveList();
        getReceiveListMap();
        getLastMsgMap();
        getLastReceiveTimeList();
    }
}
