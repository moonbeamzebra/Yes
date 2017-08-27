package ca.magenta.yes.connector.common;

import org.apache.lucene.store.Directory;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class IndexPublisher implements Runnable {

    public static final Logger logger = Logger.getLogger(IndexPublisher.class);

    private String name = null;

    private HashSet<IndexSubscriber> subscribers = new HashSet<>();

    private BlockingQueue<Directory> outboundQueue = new ArrayBlockingQueue<>(300000);

    private Thread thread = null;

    private volatile boolean doRun = true;

    private void start() {
        thread = new Thread(this, name);
        thread.start();
        logger.debug(thread.toString() + " started");
    }

    public IndexPublisher(String name) {
        this.name = name;

        this.start();

    }

    public void run() {

        logger.info("New publisher " + name + " is now running");
        try {
            while (doRun || !outboundQueue.isEmpty()) {
                Directory indexNamePath = outboundQueue.take();
                if (logger.isDebugEnabled())
                    logger.debug("Got: " + indexNamePath.toString());
                dispatch(indexNamePath);
            }

        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
            Thread.currentThread().interrupt();
        }
    }

    private synchronized void dispatch(Directory indexDir) {

        for (IndexSubscriber subscriber : subscribers) {
            try {
                subscriber.store(indexDir);
                if (logger.isDebugEnabled())
                    logger.debug("Store to " + subscriber.getName());
            } catch (InterruptedException e) {
                logger.error("Unable to post to " + subscriber.getName(), e);
                Thread.currentThread().interrupt();

            }
        }
    }

    public void publish(Directory indexDir) throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("subscribers.size: %d ; Received [%s]", subscribers.size(), indexDir));
        }
        if (!subscribers.isEmpty())
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("outboundQueue.put");
                }
                outboundQueue.put(indexDir);
            } catch (InterruptedException e) {
                if (doRun)
                    throw e;

            }
        else if (logger.isDebugEnabled())
            logger.debug("No subscribers for: " + indexDir);
    }

    synchronized void subscribe(IndexSubscriber subscriber) {

        subscribers.add(subscriber);
        if (logger.isDebugEnabled())
            logger.debug("Add subsciber:[" + subscriber.getName() + "]");

    }

    synchronized void unsubscribe(IndexSubscriber subscriber) {

        subscribers.remove(subscriber);
        if (logger.isDebugEnabled())
            logger.debug("Remove subsciber:[" + subscriber.getName() + "]");
    }
}
