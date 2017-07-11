package fr.isae.iqas.model.quality;

import fr.isae.iqas.model.observation.RawData;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by an.auger on 08/02/2017.
 */
public class MySpecificQoOAttributeComputation implements IComputeQoOAttributes {
    public MySpecificQoOAttributeComputation() {
        
    }

    @Override
    @QoOParam(name = "min_value", type = Double.class)
    @QoOParam(name = "max_value", type = Double.class)
    public Double computeQoOAccuracy(RawData observation, Map<String,Map<String, String>> qooParams) {
        Double currentValue = observation.getValue();
        Double accuracy = 0.0;

        if (qooParams.containsKey(observation.getProducer())) {
            if (qooParams.get(observation.getProducer()).containsKey("min_value") &&
                    qooParams.get(observation.getProducer()).containsKey("max_value")) {
                Double min_value = Double.valueOf(qooParams.get(observation.getProducer()).get("min_value"));
                Double max_value = Double.valueOf(qooParams.get(observation.getProducer()).get("max_value"));

                if (currentValue >= min_value && currentValue <= max_value) {
                    accuracy = 100.0;
                } else {
                    Double absoluteDist = 0.0;
                    if (currentValue < min_value) {
                        absoluteDist = min_value - currentValue;
                    } else if (currentValue > max_value) {
                        absoluteDist = currentValue - max_value;
                    }
                    if (absoluteDist >= (max_value - min_value)) {
                        accuracy = 0.0;
                    } else {
                        accuracy = ((max_value - min_value) - absoluteDist) / (max_value - min_value);
                    }
                }
            }
        }

        return accuracy;
    }

    @Override
    public Double computeQoOFreshness(RawData observation) {
        Timestamp obsSensingDate = observation.getDate();
        Double age = (double) (System.currentTimeMillis() - obsSensingDate.getTime());
        return age;
    }
}
