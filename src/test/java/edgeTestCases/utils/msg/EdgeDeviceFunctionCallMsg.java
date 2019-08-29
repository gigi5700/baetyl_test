package edgeTestCases.utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Source/Input msg of edgeTestCases rule functions
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeDeviceFunctionCallMsg {

    public String value;
    
    @JsonProperty("no_return_value")
    public String noReturnValue;
    public String timeout;
    public String exception;
}
