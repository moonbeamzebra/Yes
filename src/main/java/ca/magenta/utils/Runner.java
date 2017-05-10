package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class Runner extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected volatile boolean doRun = true;

    public Runner(String name) {

        super(name);
    }

    protected synchronized void stopIt() {
        doRun = false;
    }

    public synchronized void startInstance() throws AppException {
        doRun = true;
        this.start();
        logger.debug(String.format("%s [%s] started", this.getClass().getSimpleName(), this.getName()));
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
