package nifi.bicon.util;

import org.apache.commons.collections.map.HashedMap;

import java.util.List;
import java.util.Map;

/**
 * Created by NightWatch on 2018/1/23.
 */
public class BigMap {

    public static List<Map<String, String>> newsOldList;

    public static List<Map<String, String>> newsTodayList;

    public static Map<String, Integer> userNewsCount = new HashedMap();

}
