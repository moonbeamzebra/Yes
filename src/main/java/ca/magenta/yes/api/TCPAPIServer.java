package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.yes.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPAPIServer extends AbstractTCPServer
{

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final String indexBaseDirectory;

    public TCPAPIServer(String partitionName, Config config, int port, String indexBaseDirectory )
    {
        super(partitionName, port);

        this.indexBaseDirectory = indexBaseDirectory;
    }

    @Override
    public void run()
    {
        doRun = true;
        while(doRun)
        {
            try
            {

                Socket socket = serverSocket.accept();

                String nameStr = this.getName() + "-" + getClientCount();
                APIServer apiServer = new APIServer(this, nameStr, socket, indexBaseDirectory);
                addTcpServerHandler(apiServer);
                apiServer.start();
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
    }
}