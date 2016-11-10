package fr.isae.iqas.mapek.information;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.RunnableGraph;
import fr.isae.iqas.model.messages.Terminated;

/**
 * Created by an.auger on 13/09/2016.
 */
public class ExecuteActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorMaterializer materializer = null;
    private RunnableGraph myRunnableGraph = null;

    public ExecuteActor(Graph<ClosedShape, Materializer> graphToRun) {
        materializer = ActorMaterializer.create(getContext().system());
        myRunnableGraph = RunnableGraph.fromGraph(graphToRun);
    }

    @Override
    public void preStart() {
        if (myRunnableGraph != null) {
            myRunnableGraph.run(materializer);
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            if (myRunnableGraph != null) {
                materializer.shutdown();
            }
            getSender().tell(message, getSelf());
            getContext().system().stop(self());
        }
    }

    @Override
    public void postStop() {
        // clean up resources here ...
    }
}
