package fr.isae.iqas.pipelines;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.model.message.PipelineRequestMsg;
import org.apache.commons.codec.binary.Hex;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 02/02/2017.
 */

public class PipelineWatcherActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final List<Class> providedPipelines = Arrays.asList(new Class[] {
            IngestPipeline.class,
            FilterPipeline.class,  // OBS_ACCURACY
            ThrottlePipeline.class, // OBS_RATE
            OutputPipeline.class // OBS_FRESHNESS
    });

    private FiniteDuration rateToCheck;
    private MessageDigest md;
    private String qooPipelinesDir;
    private Map<String, String> md5Pipelines;
    private Map<String, Class> pipelineObjects;

    public PipelineWatcherActor(Config iqasConfid) {
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            log.error(e.toString());
        }
        Properties prop = iqasConfid.getProp();
        rateToCheck = new FiniteDuration(Long.parseLong(prop.getProperty("frequency_rate_to_check_seconds")), TimeUnit.SECONDS);
        qooPipelinesDir = prop.getProperty("qoo_pipelines_dir");
        md5Pipelines = new ConcurrentHashMap<>();
        pipelineObjects = new ConcurrentHashMap<>();

        for (Class c: providedPipelines) {
            pipelineObjects.put(c.getSimpleName(), c);
        }
    }

    @Override
    public void preStart() {
        checkPipelines();
        getContext().system().scheduler().scheduleOnce(
                rateToCheck,
                getSelf(), "tick", getContext().dispatcher(), null);
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, this::actionsOnStringMsg)
                .match(PipelineRequestMsg.class, this::actionsOnPipelineRequestMsg)
                .build();
    }

    private void actionsOnStringMsg(String msg) {
        if (msg.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    rateToCheck,
                    getSelf(), "tick", getContext().dispatcher(), null);

            checkPipelines();
        }
    }

    private void actionsOnPipelineRequestMsg(PipelineRequestMsg msg) {
        ArrayList<Pipeline> objectToReturn = new ArrayList<>();
        if (msg.isGetAllPipelines()) { // Only used for displaying Pipeline names on iQAS API homepage
            log.debug("PipelineRequestMsg: all available and concrete pipelines have been asked");
            pipelineObjects.forEach((k, v) -> {
                try {
                    IPipeline pipelineTemp = (IPipeline) v.newInstance();
                    if (!providedPipelines.contains(pipelineTemp.getClass())) { // If the pipeline is a custom-defined one
                        objectToReturn.add(new Pipeline(pipelineTemp.getPipelineID(), pipelineTemp.getPipelineName(), pipelineTemp));
                    }
                } catch (InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
        }
        else {
            if (pipelineObjects.containsKey(msg.getSpecificPipelineToGet())) {
                log.info("PipelineRequestMsg: concrete pipeline with id \"" + msg.getSpecificPipelineToGet() + "\" has been asked");
                try {
                    IPipeline pipelineTemp = (IPipeline) pipelineObjects.get(msg.getSpecificPipelineToGet()).newInstance();
                    objectToReturn.add(new Pipeline(pipelineTemp.getPipelineID(), pipelineTemp.getPipelineName(), pipelineTemp));
                } catch (InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        getSender().tell(objectToReturn, getSelf());
    }

    private List<String> getAllPipelineFiles() {
        List<String> filesToReturn = new ArrayList<>();
        File dirToScan = new File(qooPipelinesDir);

        if (dirToScan.exists() && dirToScan.isDirectory()) {
            String[] allFiles = dirToScan.list();
            if (allFiles != null) {
                for (String f : allFiles) {
                    // only consider files ending in ".class" equals to the wanted pipeline
                    if (f.endsWith(".class")) {
                        filesToReturn.add(f.substring(0, f.indexOf(".")));
                    }
                }
            }
        }
        return filesToReturn;
    }

    private String computeMD5(String pipelineName) {
        String md5ToReturn;

        md.reset();
        byte[] bytes = new byte[2048];
        int numBytes;
        InputStream is;
        try {
            is = Files.newInputStream(Paths.get(qooPipelinesDir + File.separator + pipelineName + ".class"));
            while ((numBytes = is.read(bytes)) != -1) {
                md.update(bytes, 0, numBytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        md5ToReturn = new String(Hex.encodeHex(md.digest()));

        return md5ToReturn;
    }

    private void checkPipelines() {
        Set<String> filesToCheck = new HashSet<>(md5Pipelines.keySet());
        List<String> listPipelines = getAllPipelineFiles();

        for (String pipelineName : listPipelines) {
            if (md5Pipelines.containsKey(pipelineName)) { // Existing pipeline, we check for changes
                if (!md5Pipelines.get(pipelineName).equals(computeMD5(pipelineName))) {
                    log.info("Content has changed for file " + pipelineName + " - reloading Pipeline");
                    md5Pipelines.put(pipelineName, computeMD5(pipelineName));
                    loadQoOPipeline(pipelineName);
                }
                filesToCheck.remove(pipelineName);
            }
            else { // Non-existing pipeline
                md5Pipelines.put(pipelineName, computeMD5(pipelineName));
                log.info("New QoO pipeline detected: " + pipelineName + " - loading Pipeline");
                loadQoOPipeline(pipelineName);
            }
        }

        for (String pipelineName : filesToCheck) { // The pipelines remaining have been removed from directory
            md5Pipelines.remove(pipelineName);
            pipelineObjects.remove(pipelineName);
            log.info("Missing QoO pipeline: " + pipelineName + " - removing Pipeline");
        }
    }

    private void loadQoOPipeline(String pipelineName) {
        File dirToScan = new File(qooPipelinesDir);
        ClassLoader cl = new PipelineClassLoader(qooPipelinesDir);
        if (dirToScan.exists() && dirToScan.isDirectory()) {
            String[] allFiles = dirToScan.list();
            if (allFiles != null) {
                for (String f : allFiles) {
                    // only consider files ending in ".class" equals to the wanted pipeline
                    if (f.equals(pipelineName + ".class")) {
                        try {
                            Class aClass = cl.loadClass(f.substring(0, f.indexOf(".")));
                            Class[] intf = aClass.getInterfaces();
                            for (Class anIntf : intf) {
                                if (anIntf.getName().equals("fr.isae.iqas.pipelines.IPipeline")) {
                                    pipelineObjects.put(pipelineName, aClass);
                                    log.info("QoO pipeline " + pipelineName + " successfully loaded from bytecode " + pipelineName + ".class");
                                }
                            }
                        } catch (Exception e) {
                            log.error("File " + f + " does not contain a valid IPipeline class!");
                            log.error(e.toString());
                        }
                    }
                }
            }
        }
    }
}
