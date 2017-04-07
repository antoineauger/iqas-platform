package fr.isae.iqas.model.quality;

import fr.isae.iqas.model.observation.RawData;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by an.auger on 08/02/2017.
 */

public interface IComputeQoOAttributes {
    @QoOParam(name = "min_value", type = Double.class)
    @QoOParam(name = "max_value", type = Double.class)
    Double computeQoOAccuracy(RawData observation, Map<String, Map<String, String>> qooParams);
    Double computeQoOFreshness(RawData observation);

    // Static method to retrieve annotations
    static Map<String, Class> getQoOParamsForInterface() {
        Map<String, Class> qooParamPrototypes = new HashMap<>();
        Method[] methods = IComputeQoOAttributes.class.getMethods();
        for (Method m : methods) {
            Annotation[] annotations = m.getAnnotations();
            for (Annotation a : annotations) {
                if (a instanceof QoOParamList) {
                    QoOParamList t = (QoOParamList) a;
                    for (int i = 0 ; i < t.value().length ; i++) {
                        qooParamPrototypes.put(t.value()[i].name()[0], t.value()[i].type()[0]);
                    }
                }
            }
        }
        return qooParamPrototypes;
    }
}
