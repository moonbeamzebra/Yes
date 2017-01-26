package ca.magenta.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;

abstract public class AbstractTCPServer extends Thread
{

    //protected final String name;

    protected ServerSocket serverSocket;
    private int port;
    protected boolean doRun = false;

    protected ArrayList<AbstractTCPServerHandler> tcpServerHandlers = new ArrayList<AbstractTCPServerHandler>();

    public AbstractTCPServer(String name, int port )
    {
        this.setName(name);
        this.port = port;
    }

    public void startServer()
    {
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

    public void stopServer()
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
            requestHandler.stopIt();
            requestHandler.closeSocket();
            requestHandler.interrupt();
        }
    }

}