package ca.magenta.utils.queuing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class MyBlockingQueue<T> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static final float DRAINING_THRESHOLD = (float) 0.30; // 70%


    private Queue<T> queue = new LinkedList<>();
    private final int capacity;

    private boolean waitTimeMode;

    MyBlockingQueue(int capacity) {
        this.capacity = capacity;
        waitTimeMode = true;
    }

    public synchronized void put(T element) throws InterruptedException {
        while(queue.size() == capacity) {
            wait();
        }

        queue.add(element);
        notifyAll(); // notifyAll() for multiple producer/consumer threads
    }

    public synchronized T take() throws InterruptedException, StopWaitAsked {
        while(queue.isEmpty()) {
            wait();
            if ( ! waitTimeMode )
            {
                throw new StopWaitAsked();
            }
        }

        T item = queue.remove();
        notifyAll(); // notifyAll() for multiple producer/consumer threads
        return item;
    }

    public int size() {
        return queue.size();
    }


    synchronized void waitForWellDrain() throws InterruptedException {

        while ( DRAINING_THRESHOLD < (queue.size() / capacity)) {
            wait();
        }
    }

    public synchronized void resetWaitState() {
        waitTimeMode = true;
    }


    public synchronized void stopWait() {
        waitTimeMode = false;
        notifyAll();
    }


    public boolean isEmpty() {
        return queue.isEmpty();
    }

    synchronized void letDrain(String ownerName) {

        logger.info("Test queue emptiness {} in {}", queue.size(), ownerName);
        while (!queue.isEmpty()) {
            if ((queue.size() % 1000) == 0) {
                logger.info("Let drain {} in {}", queue.size(), ownerName);
            }
            try {
                wait();
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
            }
        }
    }



}