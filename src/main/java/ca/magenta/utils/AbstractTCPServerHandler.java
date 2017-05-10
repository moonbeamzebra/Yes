package ca.magenta.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

abstract public class AbstractTCPServerHandler extends Runner {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final AbstractTCPServer tcpServer;

    //protected boolean doRun = false;

    protected Socket handlerSocket;

    public AbstractTCPServerHandler(AbstractTCPServer tcpServer, String name, Socket handlerSocket) {
        super(name);
        this.tcpServer = tcpServer;
        //this.setName(name);
        this.handlerSocket = handlerSocket;
    }

//    void stopIt() {
//        doRun = false;
//    }

    void closeSocket() {
        try {
            handlerSocket.close();
        } catch (IOException e) {
            logger.error("IOException", e);
        }
    }
}
