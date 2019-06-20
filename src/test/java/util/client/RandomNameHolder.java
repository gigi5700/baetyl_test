package util.client;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RandomNameHolder {

    private static Random random = new Random();
    public static Pattern pattern = Pattern.compile("^[+-]?[\\d]+([\\.][\\d]*[Ee]?[+-]?[0-9]{0,2})?$");

    /**
     * 获取ASCII表中非常见字符所构成的随机字符串 <br>
     * 取值范围 : ASCII 32 - 126, 除去"a-zA-Z0-9-_"
     *
     * @param length  随机字符串长度
     * @return    随机字符串
     */
    public static String getRandomInvalidString(int length) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < length; ++i) {
            long result = 0;

            // ASCII 32 - 126, except a-zA-Z0-9-_
            do {
                result = random.nextInt(95) + 32;
            } while (result >= 48 && result <= 57 || result >= 65 && result <= 90 || result >= 97 && result <= 122
                    || result == 45 || result == 95);
            sb.append(String.valueOf((char) result));
        }
        return sb.toString();
    }

    /**
     * 获取指定长度的随机字串
     *
     * @param length  目标字串长度
     * @return  指定长度的随机字串
     */
    public static String getRandomString(int length, String... besideNumChar) {
        // Default first char
        return getRandomString("", length, besideNumChar);
    }

    /**
     * 获取指定前缀指定长度的随机字串<br>
     * 最终生成的字串长度=前缀长度+指定长度
     *
     * @param prefix   指定前缀
     * @param length   指定除前缀外字串长度
     * @return    生成的随机字串
     */
    public static String getRandomString(String prefix, int length, String... besideNumChar) {
        StringBuffer sb = new StringBuffer(prefix);

        for (int i = 0; i < length; ++i) {
            int number = random.nextInt(3 + besideNumChar.length);
            long result = 0;

            switch (number) {
                case 0:
                    // Upper case
                    result = random.nextInt(26) + 65;
                    sb.append(String.valueOf((char) result));
                    break;
                case 1:
                    // Lower case
                    result = random.nextInt(26) + 97;
                    sb.append(String.valueOf((char) result));
                    break;
                case 2:
                    // Number
                    sb.append(String.valueOf(random.nextInt(10)));
                    break;
                default:
                    // besideNumChar
                    sb.append(besideNumChar[number - 3].charAt(random.nextInt(besideNumChar[number - 3].length())));
                    break;
            }
        }
        return sb.toString();
    }

    public static String getRandomNumberString(int length) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < length; ++i) {
            int result = random.nextInt(10);
            sb.append(result);
        }
        return sb.toString();
    }

    public static String genTopicWithSlashCount(int slashCount) {
        List<String> temp = new ArrayList<String>();
        for (int i = 0; i < slashCount + 1; i++) {
            temp.add(String.valueOf(i));
        }

        return String.join("/", temp);
    }
}
