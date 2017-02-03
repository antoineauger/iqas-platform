package fr.isae.iqas.pipelines;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.commons.codec.binary.Hex;
import scala.concurrent.duration.Duration;

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
public class PipelineWatcherActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MessageDigest md = null;
    private String qooPipelinesDir = null;
    private Map<String, String> md5Pipelines = null;
    private Set<String> filesToCheck = null;
    private Map<String, IPipeline> pipelineObjects = null;

    public PipelineWatcherActor(Properties prop) {
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            log.error(e.toString());
        }
        qooPipelinesDir = prop.getProperty("qoo_pipelines_dir");
        md5Pipelines = new ConcurrentHashMap<>();
        pipelineObjects = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(1, TimeUnit.SECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);

        System.out.println(getSelf().path().toString());
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(1, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            filesToCheck = new HashSet<>(md5Pipelines.keySet());
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
        else if (message instanceof String) {
            if (pipelineObjects.containsKey(message)) {  // The asked pipeline exists
                getSender().tell(pipelineObjects.get(message), getSelf());
            }
            else { // Otherwise the actor send back a null object
                getSender().tell("", getSelf());
            }
        }
    }

    private List<String> getAllPipelineFiles() {
        List<String> filesToReturn = new ArrayList<>();
        File dirToScan = new File(qooPipelinesDir);

        if (dirToScan.exists() && dirToScan.isDirectory()) {
            String[] allFiles = dirToScan.list();
            for (String f : allFiles) {
                // only consider files ending in ".class" equals to the wanted pipeline
                if (f.endsWith(".class")) {
                    filesToReturn.add(f.substring(0, f.indexOf(".")));
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

    private void loadQoOPipeline(String pipelineName) {
        File dirToScan = new File(qooPipelinesDir);
        ClassLoader cl = new PipelineClassLoader(qooPipelinesDir);
        if (dirToScan.exists() && dirToScan.isDirectory()) {
            String[] allFiles = dirToScan.list();
            for (String f : allFiles) {
                try {
                    // only consider files ending in ".class" equals to the wanted pipeline
                    if (!f.endsWith(".class") && !f.equals(pipelineName + ".class"))
                        continue;

                    Class aClass = cl.loadClass(f.substring(0, f.indexOf(".")));
                    Class[] intf = aClass.getInterfaces();
                    for (Class anIntf : intf) {
                        if (anIntf.getName().equals("fr.isae.iqas.pipelines.IPipeline")) {
                            IPipeline pipelineToLoad = (IPipeline) aClass.newInstance();
                            pipelineObjects.put(pipelineName, pipelineToLoad);
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
