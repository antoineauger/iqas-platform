package fr.isae.iqas.model.quality;

import java.lang.annotation.*;

/**
 * Created by an.auger on 08/02/2017.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface QoOParamList {
    QoOParam[] value();
}
