package utils.msg;

import utils.client.EdgeRuleFunctionContext;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result msg after handling of edge rule functions
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeDeviceFunctionResultMsg {

    @JsonProperty("user_name")
    public String userName;
    public EdgeRuleFunctionContext context;
    
    @JsonProperty("chinese_msg")
    public String chineseMsg = "你好";
    
    public String exception;
    public String value;

    public EdgeDeviceFunctionResultMsg(String userId, EdgeRuleFunctionContext context) {
        super();
        this.userName = userId;
        this.context = context;
    }
}
