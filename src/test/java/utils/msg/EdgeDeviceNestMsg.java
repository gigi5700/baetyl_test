package utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Nested msg for edge rule test
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeDeviceNestMsg {
    public String key;
    public String value;
    public EdgeDeviceSimpleMsg nestMsg;
}
