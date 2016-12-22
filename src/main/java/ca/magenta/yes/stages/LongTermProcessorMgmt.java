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

    public LongTermProcessorMgmt(String name, long cuttingTime, Config config) {

        this.name = name;
        this.cuttingTime = cuttingTime;
        this.inputQueue = new ArrayBlockingQueue<LogstashMessage>(300000);
        this.config = config;

    }

    public void run() {

        ObjectMapper mapper = new ObjectMapper();

        LongTermProcessor longTermProcessor = new LongTermProcessor("LongTermProcessor",
                inputQueue);

        logger.info("New LongTermProcessorMgmt running");
        try {

            while (doRun || !inputQueue.isEmpty()) {
                String indexPath = config.getIndexBaseDirectory() + File.separator;
                String indexPathName = indexPath + java.util.UUID.randomUUID();
                longTermProcessor.createIndex(indexPathName);
                Thread longTermThread = new Thread(longTermProcessor, "LongTermProcessor");
                longTermThread.start();

                Thread.sleep(cuttingTime);

                logger.info("Time to rotate...stop the thread");
                longTermProcessor.stopIt();
                longTermThread.interrupt();
                longTermThread.join();
                logger.info("Stopped");
                longTermProcessor.printReport();

                long count = longTermProcessor.getThisRunCount();
                if (count > 0) {
                    publishIndex(longTermProcessor,indexPath,indexPathName);
                }
                else
                {
                    deleteUnusedIndex(indexPathName);
                }
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }
    }

    public BlockingQueue<LogstashMessage> getInputQueue() {
        return inputQueue;
    }

    private void publishIndex(LongTermProcessor longTermProcessor,
                              String indexPath,
                              String indexPathName)
    {
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

    private void deleteUnusedIndex(String indexPathName)
    {
        File index = new File(indexPathName);
        String[]entries = index.list();
        for(String s: entries){
            logger.info(String.format("Delete [%s]", s));
            File currentFile = new File(index.getPath(),s);
            currentFile.delete();
            logger.info(String.format("Done"));
        }
        String s = indexPathName;
        logger.info(String.format("Delete dir [%s]", indexPathName));
        index.delete();
        logger.info(String.format("Done"));
    }


}
