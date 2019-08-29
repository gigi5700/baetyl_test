package edgeTestCases.utils.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RuleDataMsg
 *
 * @author Ye Xiang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleDataMsg extends RuleSimpleMsg<String> {

    private String ce;
    private String cne;
    private int cv1;
    private int cv2;
    private String cv3;

    public RuleDataMsg(String value, String ce, String cne, int cv1, int cv2, String cv3) {
        super(value, value, value, value);
        this.ce = ce;
        this.cne = cne;
        this.cv1 = cv1;
        this.cv2 = cv2;
        this.cv3 = cv3;
    }

    public String toString() {
        return super.toString();
    }
}
