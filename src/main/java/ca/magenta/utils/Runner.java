package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Runner extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public boolean isDoRun() {
        return doRun;
    }

    public void setDoRun(boolean doRun) {
        this.doRun = doRun;
    }

    private volatile boolean doRun = true;

    public Runner(String name) {

        super(name);
    }

    protected synchronized void stopIt() {
        setDoRun(false);
    }

    public synchronized void startInstance() throws AppException {
        doRun = true;
        this.start();
        if (logger.isDebugEnabled()) {
            logger.debug("{} [{}] started", this.getClass().getSimpleName(), this.getName());
        }
    }

//    public void gentlyStopInstance(long millis) {
//        doRun = false;
//        try {
//            logger.info("{} [{}] gently ask to stop; wait %d millis", this.getClass().getSimpleName(), this.getName(), millis);
//            this.join(millis);
//        } catch (InterruptedException e) {
//            logger.error("InterruptedException", e);
//            Thread.currentThread().interrupt();
//        }
//
//        if (logger.isDebugEnabled()) {
//            logger.debug("{} [{}] gently ask to stop", this.getClass().getSimpleName(), this.getName());
//        }
//    }

//    public void gentlyStopInstance() {
//        gentlyStopInstance(500);
//    }

    public synchronized void stopInstance() {
        stopIt();
        try {
            this.join();
        } catch (InterruptedException e) {
            logger.error(e.getClass().getSimpleName(), e);
            Thread.currentThread().interrupt();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("{} [{}] stopped", this.getClass().getSimpleName(), this.getName());
        }
    }

}
