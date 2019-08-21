package utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Simple msg for edge rule test
 *
 * @author Zhao Meng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EdgeDeviceSimpleMsg {
    public String device;
    public String data;
    public int value;
    public boolean boolValue;   
    public double doubleValue = 0.1;
    
    public EdgeDeviceSimpleMsg(String device, String data) {
        this.device = device;
        this.data = data;
    }
}
