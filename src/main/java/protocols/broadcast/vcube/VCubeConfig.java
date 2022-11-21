package protocols.broadcast.vcube;


import java.util.*;

public class VCubeConfig {
    static public Map<Integer, HashSet<Integer>> nodeIdsByTopic;

    static {
        HashMap<Integer, HashSet<Integer>> sourceMap = new HashMap<>();
        sourceMap.put(1,  new HashSet<>(Arrays.asList(19, 37, 16, 32, 20, 12, 30, 1, 47, 40, 5, 45, 33, 10, 39, 9, 11, 7, 48, 41, 8, 26, 25, 15, 34, 44, 4, 27, 29, 23, 2, 49, 13, 22, 42, 21, 17, 38, 3, 0, 24, 46, 18, 36)));
        sourceMap.put(2,  new HashSet<>(Arrays.asList(26, 37, 49, 11, 21, 38, 44, 45, 46)));
        sourceMap.put(3,  new HashSet<>(Arrays.asList(18, 6, 32, 16, 15, 1, 27, 34, 25, 21, 44, 39, 0, 48, 7, 12, 29, 9, 43, 33, 42, 24, 8, 20, 3, 17, 5, 11)));
        sourceMap.put(4,  new HashSet<>(Arrays.asList(21, 22, 36, 3, 42, 11, 5, 16, 26, 8, 24, 15, 1, 49, 44, 25, 29, 47)));
        nodeIdsByTopic = Collections.unmodifiableMap(sourceMap);
    }

}
