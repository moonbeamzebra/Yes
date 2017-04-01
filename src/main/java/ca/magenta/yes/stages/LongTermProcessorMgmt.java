package ca.magenta.yes.stages;


import ca.magenta.utils.AppException;
import ca.magenta.yes.Globals;
import ca.magenta.yes.data.MasterIndexRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;


class LongTermProcessorMgmt extends ProcessorMgmt {

    private static final Logger logger = LoggerFactory.getLogger(LongTermProcessorMgmt.class.getPackage().getName());

    private final String masterIndexPathName;

    LongTermProcessorMgmt(String name, long cuttingTime, String partition) {
        super(name, partition, cuttingTime);

        masterIndexPathName = Globals.getConfig().getIndexBaseDirectory() +
                File.separator +
                "master.lucene";

    }

    synchronized void publishIndex(Processor longTermProcessor,
                                   String indexPath,
                                   String indexPathName, String partition) throws IOException, AppException {
        String newFileName = String.format("%s-%d.run.%d-%d.lucene",
                this.partition,
                longTermProcessor.getRunTimeStamps().getNewerRxTimestamp(),
                longTermProcessor.getRunTimeStamps().getRunStartTimestamp(),
                longTermProcessor.getRunTimeStamps().getRunEndTimestamp());
        String newIndexPathName = indexPath + newFileName;
        File dir = new File(indexPathName);
        File newDirName = new File(newIndexPathName);
        if (dir.isDirectory()) {
            if (dir.renameTo(newDirName)) {
                MasterIndexRecord.updateMasterIndex(masterIndexPathName, newFileName, longTermProcessor.getRunTimeStamps(),partition);
                logger.info(String.format("Index [%s] published", newFileName));
            } else {
                logger.error(String.format("Cannot rename [%s to %s]", indexPathName, newIndexPathName));
            }
        } else {
            logger.error(String.format("Unexpected error; [%s] is not a directory", newIndexPathName));
        }
    }


    synchronized void deleteUnusedIndex(String indexPathName) {
        File index = new File(indexPathName);
        String[] entries = index.list();
        if (entries != null) {
            for (String s : entries) {
                logger.info(String.format("Delete [%s]", s));
                File currentFile = new File(index.getPath(), s);
                if (currentFile.delete())
                    logger.info("Done");
                else
                    logger.error(String.format("Cannot delete [%s/%s]", index.getPath(), s));
            }
        }
        logger.info(String.format("Delete dir [%s]", indexPathName));
        if (index.delete())
            logger.info("Done");
        else
            logger.error(String.format("Cannot delete [%s]", indexPathName));
    }


    Processor createProcessor(BlockingQueue<Object> queue) throws AppException {

        return new LongTermProcessor("LongTermProcessor", partition, queue);

    }

}
