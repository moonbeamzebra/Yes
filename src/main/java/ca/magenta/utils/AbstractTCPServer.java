package ca.magenta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

abstract public class AbstractTCPServer extends Thread
{

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    protected ServerSocket serverSocket;
    private int port;
    protected boolean doRun = false;

    private HashMap<String, AbstractTCPServerHandler> tcpServerHandlers = new HashMap<String, AbstractTCPServerHandler>();

    public AbstractTCPServer(String name, int port )
    {
        this.setName(name);
        this.port = port;
    }

    public void startServer() throws AppException {
        try
        {
            serverSocket = new ServerSocket( port );
            this.start();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    synchronized public void stopServer()
    {
        doRun = false;
        this.interrupt();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, AbstractTCPServerHandler> requestHandlerSet : tcpServerHandlers.entrySet())
        {
            AbstractTCPServerHandler requestHandler = requestHandlerSet.getValue();
            if ( requestHandler != null ) {
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