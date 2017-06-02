package fr.isae.iqas.model.message;

import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.pipelines.IPipeline;

import java.util.Set;

import static fr.isae.iqas.model.message.MAPEKenums.ActionMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class ActionMsgPipeline extends ActionMsg {
    private final ObservationLevel askedObsLevel;
    private final IPipeline pipelineToEnforce;
    private final Set<String> topicsToPullFrom;
    private final String topicToPublish;
    private final String associatedRequest_id;
    private final String constructedFromRequest;
    private final int maxLevelDepth;

    public ActionMsgPipeline(ActionMAPEK action, EntityMAPEK about,
                             IPipeline pipelineToEnforce,
                             ObservationLevel askedObsLevel,
                             Set<String> topicsToPullFrom,
                             String topicToPublish,
                             String associatedRequest_id,
                             String constructedFromRequest,
                             int maxLevelDepth) {
        super(action, about);
        this.askedObsLevel = askedObsLevel;
        this.pipelineToEnforce = pipelineToEnforce;
        this.topicsToPullFrom = topicsToPullFrom;
        this.topicToPublish = topicToPublish;
        this.associatedRequest_id = associatedRequest_id;
        this.constructedFromRequest = constructedFromRequest;
        this.maxLevelDepth = maxLevelDepth;
    }

    public ObservationLevel getAskedObsLevel() {
        return askedObsLevel;
    }

    public IPipeline getPipelineToEnforce() {
        return pipelineToEnforce;
    }

    public Set<String> getTopicsToPullFrom() {
        return topicsToPullFrom;
    }

    public String getTopicToPublish() {
        return topicToPublish;
    }

    public String getAssociatedRequest_id() {
        return associatedRequest_id;
    }

    public String getConstructedFromRequest() {
        return constructedFromRequest;
    }

    public int getMaxLevelDepth() {
        return maxLevelDepth;
    }
}
