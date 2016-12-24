package ca.magenta.yes.stages;


import ca.magenta.yes.Config;
import ca.magenta.yes.data.LogstashMessage;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.BlockingQueue;


public class LongTermProcessorMgmt extends ProcessorMgmt {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());

    public LongTermProcessorMgmt(String name, long cuttingTime, Config config) {
        super(name, cuttingTime, config);

    }

    synchronized  void publishIndex(Processor longTermProcessor,
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
        if ( dir.isDirectory() ) {
            dir.renameTo(newDirName);
            logger.info(String.format("Index [%s] published", newFileName));
        }
        else
        {
            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
        }
    }

    synchronized  void deleteUnusedIndex(String indexPathName)
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

    Processor createProcessor(BlockingQueue<LogstashMessage> queue) {

        return new LongTermProcessor("LongTermProcessor",
                queue);

    }


}
