package ca.magenta.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

abstract public class AbstractTCPServer extends Thread {

    protected ServerSocket serverSocket;
    private int port;
    protected boolean doRun = false;

    private HashMap<String, AbstractTCPServerHandler> tcpServerHandlers = new HashMap<>();

    public AbstractTCPServer(String name, int port) {
        this.setName(name);
        this.port = port;
    }

    public void startServer() throws AppException {
        try {
            serverSocket = new ServerSocket(port);
            this.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    synchronized public void stopServer() {
        doRun = false;
        this.interrupt();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
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

    synchronized public int getClientCount() {
        return tcpServerHandlers.size();
    }

}