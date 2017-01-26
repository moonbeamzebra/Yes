package ca.magenta.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jplaberge on 2017-01-23.
 */
abstract public class ThreadRunnable implements Runnable {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private Thread runner = null;
    private final String runnerName;
    protected volatile boolean doRun = true;

    public ThreadRunnable(String runnerName) {
        this.runnerName = runnerName;
    }

    private synchronized void stopIt() {
        doRun = false;
    }

    public synchronized void startInstance() {
        if (runner == null) {
            runner = new Thread(this, runnerName);
            runner.start();
            logger.info(String.format("%s [%s] started", this.getClass().getSimpleName(), runnerName));
        }
    }

    public synchronized void stopInstance() {
        if (runner != null) {
            stopIt();
            runner.interrupt();
            try {
                runner.join();
            } catch (InterruptedException e) {
                logger.error("InterruptedException", e);
            }
            String rName = runner.getName();
            runner = null;

            logger.info(String.format("%s [%s] stopped", this.getClass().getSimpleName(), runnerName));
        }
    }


}
