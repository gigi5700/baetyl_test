package utils.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Connection type for apollo.
 *
 * @author Zhao Meng
 */
public enum ConnectionType {
    SSL, TCP, WS, WSS;
    
    public static Random random = new Random();
    
    public static ConnectionType getAnotherType(ConnectionType type) {
        List<ConnectionType> types = new ArrayList<ConnectionType>(Arrays.asList(ConnectionType.values()));
        types.remove(type);
        return types.get(random.nextInt(types.size()));
    }
}
