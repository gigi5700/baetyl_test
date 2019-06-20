package util.client.fusesource;

import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.Callback;

/**
 * Fuse MQTT Call backs<br>
 * 用于Fuse库的回调函数
 *
 * @author Ye Xiang(yexiang01@baidu.com)
 */
@Slf4j
public class FuseCallbacks implements Callback {

    private String callBackName;

    public FuseCallbacks(String name) {
        callBackName = name;
    }

    @Override
    public void onSuccess(Object value) {
        log.info("{} SUCCESS", callBackName);
    }

    @Override
    public void onFailure(Throwable value) {
        log.info("{} FAILED: {}", callBackName, value.getMessage());
        value.printStackTrace();
    }
}
