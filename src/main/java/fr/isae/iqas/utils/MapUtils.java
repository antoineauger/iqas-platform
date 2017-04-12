package fr.isae.iqas.utils;

import java.util.Map;
import java.util.Objects;

/**
 * Created by an.auger on 12/04/2017.
 */
public class MapUtils {

    /**
     * Util method to find the associated key to a given value
     * @param map the Map to search into
     * @param value the value to search for
     * @param <T> type for Map keys
     * @param <E> type for Map values
     * @return the first corresponding entry for the specified key or null
     */
    public static<T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Map.Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

}
