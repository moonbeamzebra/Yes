package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.utils.AppException;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class TCPGenericConnector extends AbstractTCPServer
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final LogParser logParser;

    private final Config config;

    public TCPGenericConnector(String partitionName, Config config, int port )
    {
        super(partitionName, port);

        this.config = config;

        logParser = new LogParser(partitionName, config, partitionName);

    }

    @Override
    public void startServer() throws AppException {
        //Start drains
        logParser.startInstance();

        super.startServer();
    }

    @Override
    synchronized public void stopServer()
    {
        super.stopServer();

        //Stop drains


    }


    @Override
    public void run()
    {
        logger.info(String.format("Start listen on port [%d]", serverSocket.getLocalPort() ));

        doRun = true;
        while(doRun)
        {
            try
            {

                Socket socket = serverSocket.accept();

                String nameStr = this.getName() + "-" + getClientCount();
                GenericConnector genericConnector = new GenericConnector(this, nameStr, config, socket, logParser );
                addTcpServerHandler(genericConnector);
                genericConnector.start();
            }
            catch (SocketException e)
            {
                if (doRun) {
                    logger.error("SocketException", e);
                }
            }
            catch (IOException e)
            {
                logger.error("IOException", e);
            }
        }
        logger.info(String.format("Stop listen on port [%d]", serverSocket.getLocalPort() ));
    }
}