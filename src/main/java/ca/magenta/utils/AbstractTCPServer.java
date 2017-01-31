package ca.magenta.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;

abstract public class AbstractTCPServer extends Thread
{


    protected ServerSocket serverSocket;
    private int port;
    protected boolean doRun = false;

    private HashSet<AbstractTCPServerHandler> tcpServerHandlers = new HashSet<AbstractTCPServerHandler>();

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

        for (AbstractTCPServerHandler requestHandler : tcpServerHandlers)
        {
            if ( requestHandler != null ) {
                requestHandler.stopIt();
                requestHandler.closeSocket();
                requestHandler.interrupt();
            }
        }
    }

    synchronized protected void addTcpServerHandler(AbstractTCPServerHandler abstractTCPServerHandler) {
        tcpServerHandlers.add(abstractTCPServerHandler);
    }

    public synchronized void removeTcpServerHandler(AbstractTCPServerHandler abstractTCPServerHandler) {
        tcpServerHandlers.remove(abstractTCPServerHandler);
    }

    synchronized public int getClientCount() {
        return tcpServerHandlers.size();
    }

}