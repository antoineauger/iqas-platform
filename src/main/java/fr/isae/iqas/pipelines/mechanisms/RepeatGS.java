package fr.isae.iqas.pipelines.mechanisms;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * This QoO mechanism allows to repeat every windowDuration the last element seen (if any).
 *
 * @param <T> the type of the Objects to pull/push
 */
public class RepeatGS<T> extends GraphStage<FlowShape<T, T>> {

    private final FiniteDuration windowDuration;

    public RepeatGS(FiniteDuration windowDuration) {
        this.windowDuration = windowDuration;
    }

    public final Inlet<T> in = Inlet.create("Repeat.in");
    public final Outlet<T> out = Outlet.create("Repeat.out");
    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new TimerGraphStageLogic(shape) {

            private T currentValue = null;
            private long lastEmission = 0;

            {

                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        currentValue = grab(in);
                        lastEmission = System.currentTimeMillis();
                        push(out, currentValue);
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if (!hasBeenPulled(in)) {
                            pull(in);
                        }
                    }
                });
            }

            @Override
            public void preStart() {
                schedulePeriodically("timeToEmit", new FiniteDuration(1, TimeUnit.SECONDS));
            }

            @Override
            public void onTimer(Object key) {
                if (key.equals("timeToEmit")) {
                    long now = System.currentTimeMillis();
                    if ((now - lastEmission >= windowDuration.toMillis()) && currentValue != null) {
                        push(out, currentValue);
                        lastEmission = now;
                    }
                }
            }
        };
    }
}
