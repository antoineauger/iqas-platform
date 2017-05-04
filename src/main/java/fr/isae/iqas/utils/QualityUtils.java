package fr.isae.iqas.utils;

/**
 * Created by an.auger on 04/05/2017.
 */
public class QualityUtils {
    public static String inverseOfVariation(String var) {
        switch (var) {
            case "HIGH":
                return "LOW";
            case "LOW":
                return "HIGH";
            default:
                return var;
        }
    }
}
