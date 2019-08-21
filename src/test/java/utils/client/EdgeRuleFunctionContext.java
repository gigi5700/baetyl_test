package utils.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Context model for edge function
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeRuleFunctionContext {

    public String functionName;
    public String functionInvokeId;
    public String functionInstanceId;
    public String invokeid;
    public String messageId;
    public int messageQOS;
    public String messageTopic;
    public boolean messageRetain;

    public EdgeRuleFunctionContext(String functionName, int messageQos, String messageTopic, boolean messageRetain) {
        super();
        this.functionName = functionName;
        this.messageQOS = messageQos;
        this.messageTopic = messageTopic;
        this.messageRetain = messageRetain;
    }
}
