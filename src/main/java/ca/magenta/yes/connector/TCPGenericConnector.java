package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPGenericConnector extends AbstractTCPServer
{
    private final LogParser logParser;

    private final Config config;
    private final RealTimeProcessorMgmt realTimeProcessorMgmt;

    public TCPGenericConnector(String partitionName, Config config, int port, RealTimeProcessorMgmt realTimeProcessorMgmt )
    {
        super(partitionName, port);

        this.config = config;
        this.realTimeProcessorMgmt =  realTimeProcessorMgmt;

        logParser = new LogParser(partitionName, config, realTimeProcessorMgmt, partitionName);
        logParser.startInstance();

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

                // Call accept() to receive the next connection
                Socket socket = serverSocket.accept();

                // Pass the handlerSocket to the utils.AbstractTCPServerHandler thread for processing
                GenericConnector genericConnector = new GenericConnector(this.getName(), config, socket, logParser );
                tcpServerHandlers.add(genericConnector);
                genericConnector.start();
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