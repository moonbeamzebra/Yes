package ca.magenta.yes.connector.common;

import org.apache.lucene.store.Directory;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-11-29
 */
public class IndexPublisher implements Runnable {

    public static Logger logger = Logger.getLogger(IndexPublisher.class);

    private String name = null;

    private HashSet<IndexSubscriber> subscribers = new HashSet<IndexSubscriber>();

    private BlockingQueue<Directory> outboundQueue = new ArrayBlockingQueue<Directory>(300000);

    private Thread thread = null;

    private volatile boolean doRun = true;

    public void stop() {
        doRun = false;
    }

    public void start() {
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
        }
    }

    synchronized private void dispatch(Directory indexDir) {

        for (IndexSubscriber subscriber : subscribers) {
            try {
                subscriber.store(indexDir);
                if (logger.isDebugEnabled())
                    logger.debug("Store to " + subscriber.getName());
            } catch (InterruptedException e) {
                logger.error("Unable to post to " + subscriber.getName(), e);
            }
        }
    }

    public void publish(Directory indexDir) throws InterruptedException {
        if (logger.isDebugEnabled())
            logger.debug(String.format("Received [%s]", indexDir));
        if (!subscribers.isEmpty())
            try {
                outboundQueue.put(indexDir);
            } catch (InterruptedException e) {
                if (doRun)
                    throw e;

            }
        else if (logger.isDebugEnabled())
            logger.debug("No subscribers for: " + indexDir);
    }

    synchronized public void subscribe(IndexSubscriber subscriber) {

        subscribers.add(subscriber);
        if (logger.isDebugEnabled())
            logger.debug("Add subsciber:[" + subscriber.getName() + "]");

    }

    synchronized public void unsubscribe(IndexSubscriber subscriber) {

        subscribers.remove(subscriber);
        if (logger.isDebugEnabled())
            logger.debug("Remove subsciber:[" + subscriber.getName() + "]");
    }
}
