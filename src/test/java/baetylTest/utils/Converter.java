package baetylTest.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json与Bean的转换工具类
 *
 * @author Ye Xiang
 */
@Slf4j
public class Converter {

    public static ObjectMapper om = new ObjectMapper();
    public static Random random = new Random();

    static {
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        om.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        om.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    /**
     * 将json转换为Bean
     *
     * @param jsonValue   Json字串
     * @param toValueType   目标Bean类型
     * @param <T>       目标Bean类
     * @return   填充完成的目标Bean
     * @throws Exception
     */
    public static <T> T jsonToModel(String jsonValue, Class<T> toValueType) throws Exception {
        return om.readValue(jsonValue, toValueType);
    }

    /**
     * 将Bean转换为Json字串
     *
     * @param model  目标Bean实例
     * @param <T>    目标Bean类型
     * @return       Bean转换而来的Json字串
     * @throws JsonProcessingException
     */
    public static <T> String modelToJson(T model) throws JsonProcessingException {
        return om.writeValueAsString(model);
    }

    public static <T> String modelToJsonUsingJsonNode (T model) throws JsonProcessingException {
        return om.valueToTree(model).toString();
    }

    /**
     * 将json串转换为Bean，转换时不转换json里为null的值
     *
     * @deprecated 弃用
     * @param jsonValue
     * @param toValueType
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T jsonToModelWithoutNull(String jsonValue, Class<T> toValueType) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        return objectMapper.readValue(jsonValue, toValueType);
    }

    /**
     * 将bean换为Json串，转换时不转换bean里为null的值
     *
     * @deprecated 弃用
     * @param model
     * @param <T>
     * @return
     * @throws JsonProcessingException
     */
    public static <T> String modelToJsonWithoutNull(T model) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        return objectMapper.writeValueAsString(model);
    }

    /**
     * 复制给定路径文件到临时文件
     *
     * @deprecated 弃用
     * @param filePath
     * @return
     * @throws IOException
     */
    public static String copyToTempFile(String filePath) throws IOException {
        FileInputStream srcFileStream = new FileInputStream(new File(filePath));

        File tmpFile = File.createTempFile("SystemTest", "ca");
        if (!tmpFile.exists()) {
            return null;
        }

        FileOutputStream tmpFileStream = new FileOutputStream(tmpFile);

        FileChannel inputCh = srcFileStream.getChannel();
        FileChannel outCh = tmpFileStream.getChannel();

        inputCh.transferTo(0, inputCh.size(), outCh);

        return tmpFile.getAbsolutePath();
    }

    /**
     * Bean转换为Map
     *
     * @param model   目标Bean实例
     * @param <T>     目标Bean类型
     * @return        转换来的Map
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static <T> Map<String, Object> modelToMap(T model) throws Exception {
        return jsonToModel(modelToJson(model), Map.class);
    }
}