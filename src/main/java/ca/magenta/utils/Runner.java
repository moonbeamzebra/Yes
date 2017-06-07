package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class Runner extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected volatile boolean doRun = true;

    public Runner(String name) {

        super(name);
    }

    //public void gentleStopIt() {
    //    doRun = false;
    //}

    protected synchronized void stopIt() {
        doRun = false;
    }

    public synchronized void startInstance() throws AppException {
        doRun = true;
        this.start();
        logger.debug(String.format("%s [%s] started", this.getClass().getSimpleName(), this.getName()));
    }

    public void gentlyStopInstance(long millis) {
        doRun = false;
        try {
            logger.info(String.format("%s [%s] gently ask to stop; wait %d millis", this.getClass().getSimpleName(), this.getName(), millis));
            this.join(millis);
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }

        logger.debug(String.format("%s [%s] gently ask to stop", this.getClass().getSimpleName(), this.getName()));
    }

    public void gentlyStopInstance() {
        gentlyStopInstance(500);
    }

    public synchronized void stopInstance() {
        stopIt();
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        }

        logger.debug(String.format("%s [%s] stopped", this.getClass().getSimpleName(), this.getName()));
    }

    public boolean isDoRun() {
        return doRun;
    }
}
