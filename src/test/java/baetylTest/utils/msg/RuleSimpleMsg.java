package baetylTest.utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RuleSimpleMsg
 *
 * @author Ye Xiang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleSimpleMsg<T> {

    private String key;
    private T message;
    private String metric;
    private T value;
    private long timestamp;
    private T other;

    public RuleSimpleMsg(String key, T message, String metric, T value) {
        this.key = key;
        this.metric = metric;
        this.message = message;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
        this.other = message;
    }

  /*  public String toString() {
        try {
            return Converter.modelToJson(this) + "\n";
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
    }*/
}
