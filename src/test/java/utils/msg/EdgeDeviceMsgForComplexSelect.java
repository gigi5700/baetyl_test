package utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Msg for complex select cause test in edge rule
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeDeviceMsgForComplexSelect {
    public String key;
    public String value;
    public String data;
    public int boolValue;
}
