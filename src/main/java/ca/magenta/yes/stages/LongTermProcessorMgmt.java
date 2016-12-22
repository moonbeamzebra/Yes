package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.data.LogstashMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessorMgmt implements Runnable {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final String name;

    private final long cuttingTime;
    private final Config config;


    private final BlockingQueue<LogstashMessage> inputQueue;

    private volatile boolean doRun = true;

    public void stopIt() {
        doRun = false;
    }



    private static final long printEvery = 100;

    private long count = 0;

    public LongTermProcessorMgmt(String name, long cuttingTime, Config config) {

        this.name = name;
        this.cuttingTime = cuttingTime;
        this.inputQueue = new ArrayBlockingQueue<LogstashMessage>(300000);
        this.config = config;

    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();


        logger.info("New LongTermProcessorMgmt running");
        count = 0;
        long previousNow = System.currentTimeMillis();
        long now;
        long totalTime;
        float msgPerSec;
        try {

            while (doRun || !inputQueue.isEmpty()) {
                String indexPath = config.getIndexBaseDirectory() + File.separator;
                String indexPathName = indexPath + java.util.UUID.randomUUID();
                LongTermProcessor longTermProcessor = new LongTermProcessor("LongTermProcessor",
                        inputQueue,
                        indexPathName);
                Thread longTermThread = new Thread(longTermProcessor);
                longTermThread.start();

                Thread.sleep(cuttingTime);
                logger.debug("Time to stop");
                longTermProcessor.stopIt();
                longTermThread.interrupt();
                longTermThread.join();
                logger.debug("Stopped");
                long count = longTermProcessor.getCount();
                if (count > 0) {
                    long olderTimestamp = longTermProcessor.getOlderTimestamp();
                    long newerTimestamp = longTermProcessor.getOlderTimestamp();
                    String newFileName = "" + olderTimestamp + "-" +
                            newerTimestamp + ".cut." +
                            System.currentTimeMillis() +
                            ".lucene";
                    String newIndexPathName = indexPath + newFileName;
                    File dir = new File(indexPathName);
                    File newDirName = new File(newIndexPathName);
                    if ( dir.isDirectory() )
                        dir.renameTo(newDirName);
                }
                else
                {
                    File index = new File(indexPathName);
                    String[]entries = index.list();
                    for(String s: entries){
                        File currentFile = new File(index.getPath(),s);
                        currentFile.delete();
                    }
                }
                longTermProcessor = null;
                longTermThread = null;

            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }

    public BlockingQueue<LogstashMessage> getInputQueue() {
        return inputQueue;
    }


}
