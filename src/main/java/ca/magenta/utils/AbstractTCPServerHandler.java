package ca.magenta.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

abstract public class AbstractTCPServerHandler extends Thread
{

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());


    protected boolean doRun = false;

    protected Socket handlerSocket;
    public AbstractTCPServerHandler(String name, Socket handlerSocket )
    {
        this.setName(name);
        this.handlerSocket = handlerSocket;
    }

    public void stopIt()
    {
        doRun = false;
    }

    public void closeSocket()
    {
        try {
            handlerSocket.close();
        } catch (IOException e) {
            logger.error("IOException", e);
        }
    }
}
