package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.connector.GenericConnector;
import ca.magenta.yes.connector.LogParser;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPAPIServer extends AbstractTCPServer
{
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
                System.out.println( "Listening for a connection" );

                Socket socket = serverSocket.accept();
                setClientCount(getClientCount() + 1);

                String nameStr = this.getName() + "-" + getClientCount();
                APIServer apiServer = new APIServer(this, nameStr, socket, indexBaseDirectory);
                addTcpServerHandler(apiServer);
                apiServer.start();
            }
            catch (SocketException e)
            {
                if (doRun) {
                    System.err.println("SocketException");
                    e.printStackTrace();
                }
            }
            catch (IOException e)
            {
                System.err.println( "IOException" );
                e.printStackTrace();
            }
        }
    }
}