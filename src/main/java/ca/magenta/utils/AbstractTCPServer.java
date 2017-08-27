package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTCPServer extends Runner {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());


    protected ServerSocket serverSocket;
    private int port;

    private HashMap<String, AbstractTCPServerHandler> tcpServerHandlers = new HashMap<>();

    public AbstractTCPServer(String threadName, int port) {
        super(threadName);
        this.port = port;
    }

    // AppException can be thrown by override methods
    public void startServer() throws AppException {
        try {
            serverSocket = new ServerSocket(port);
            this.start();
        } catch (IOException e) {
            logger.error(e.getClass().getSimpleName(), e);
        }
    }

    public synchronized void stopServer() {
        stopIt();
        this.interrupt();
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error(e.getClass().getSimpleName(), e);
        }

        for (Map.Entry<String, AbstractTCPServerHandler> requestHandlerSet : tcpServerHandlers.entrySet()) {
            AbstractTCPServerHandler requestHandler = requestHandlerSet.getValue();
            if (requestHandler != null) {
                requestHandler.stopIt();
                requestHandler.closeSocket();
                requestHandler.interrupt();
            }
        }
    }

    protected void addTcpServerHandler(AbstractTCPServerHandler abstractTCPServerHandler) {
        tcpServerHandlers.put(abstractTCPServerHandler.getName(), abstractTCPServerHandler);
    }

    public void removeTcpServerHandler(AbstractTCPServerHandler abstractTCPServerHandler) {
        tcpServerHandlers.remove(abstractTCPServerHandler.getName());
    }

    public synchronized int getClientCount() {
        return tcpServerHandlers.size();
    }

}