package fr.isae.iqas.model.quality;

import fr.isae.iqas.model.observation.Information;

import java.sql.Timestamp;

import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;

/**
 * Created by an.auger on 08/02/2017.
 */
public class MySpecificQoOAttributeComputation implements IComputeQoOAttributes {
    public MySpecificQoOAttributeComputation() {
        
    }

    @Override
    @QoOParam(name = "min_value", type = Double.class)
    @QoOParam(name = "max_value", type = Double.class)
    public Information computeQoOAccuracy(Information information, Double min_value, Double max_value) {
        Double currentValue = information.getValue();
        Double accuracy;

        if (currentValue >= min_value  && currentValue <= max_value) {
            accuracy = 100.0;
        }
        else {
            Double absoluteDist = 0.0;
            if (currentValue < min_value) {
                absoluteDist = min_value - currentValue;
            }
            else if (currentValue > max_value) {
                absoluteDist = currentValue - max_value;
            }
            if (absoluteDist >= (max_value - min_value)) {
                accuracy = 0.0;
            }
            else {
                accuracy = ((max_value - min_value) - absoluteDist) / (max_value - min_value);
            }
        }

        information.setQoOAttribute(OBS_ACCURACY, accuracy);
        return information;
    }

    @Override
    public Information computeQoOFreshness(Information information) {
        Timestamp informationSensingDate = information.getTimestamp();
        Double age = (double) (System.currentTimeMillis() - informationSensingDate.getTime());

        information.setQoOAttribute(OBS_FRESHNESS, age);
        return information;
    }
}
