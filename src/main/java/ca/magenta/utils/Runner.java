package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Runner extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public boolean isDoRun() {
        return doRun;
    }

    protected void setDoRun(boolean doRun) {
        this.doRun = doRun;
    }

    private volatile boolean doRun = true;

    public Runner(String name) {

        super(name);
    }

    protected synchronized void stopIt() {
        setDoRun(false);
    }

    // AppException can be thrown by override methods
    public synchronized void startInstance() throws AppException {
        doRun = true;
        this.start();
        if (logger.isDebugEnabled()) {
            logger.debug("{} [{}] started", this.getClass().getSimpleName(), this.getName());
        }
    }

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

    @Override
    public void run() {}
}
