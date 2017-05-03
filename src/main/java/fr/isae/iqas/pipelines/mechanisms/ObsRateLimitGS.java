package fr.isae.iqas.pipelines.mechanisms;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import scala.concurrent.duration.FiniteDuration;

/**
 * This QoO mechanism allows to limit the observation rate per unit of time without introducing any delay (pass through or drop).
 * If the maximum number of elements per unit of time has been reached, new elements are dropped until the next window.
 *
 * @param <T> the type of the Objects to pull/push
 */
public class ObsRateLimitGS<T> extends GraphStage<FlowShape<T, T>> {

    private final FiniteDuration referencePeriod;
    private int nbElemsMax;

    public ObsRateLimitGS(int nbElemsMax, FiniteDuration referencePeriod) {
        this.referencePeriod = referencePeriod;
        this.nbElemsMax = nbElemsMax;
    }

    public final Inlet<T> in = Inlet.create("ObsRateLimit.in");
    public final Outlet<T> out = Outlet.create("ObsRateLimit.out");
    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new TimerGraphStageLogic(shape) {

            private boolean open = false;
            private int countElements = 0;

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        T elem = grab(in);
                        if (open || countElements >= nbElemsMax) {
                            pull(in);
                        }
                        else {
                            if (countElements == 0) {
                                scheduleOnce("resetCounter", referencePeriod);
                            }
                            push(out, elem);
                            countElements += 1;
                        }
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        pull(in);
                    }
                });
            }

            @Override
            public void onTimer(Object key) {
                if (key.equals("resetCounter")) {
                    open = false;
                    countElements = 0;
                }
            }
        };
    }
}
