package ca.magenta.yes.connector;

import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.yes.Config;
import ca.magenta.yes.stages.RealTimeProcessorMgmt;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;



/**
 * Created by jean-paul.laberge on 12/19/2016.
 */
public class GenericConnector extends AbstractTCPServerHandler {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getPackage().getName());


    private final String partition;

    private final LogParser logParser;

    public GenericConnector(String partitionName, Config config, Socket handlerSocket, LogParser logParser) {

        super(partitionName, handlerSocket);

        this.partition = partitionName;

        this.logParser = logParser;


        //logger.info(String.format("GenericConnector started on port [%d] for partion [%s]", genericConnectorPort, partition));

    }


//    public void runOld(Socket data) {
//
//        BufferedReader in = null;
//        try {
//            in = new BufferedReader(new InputStreamReader(data.getInputStream()));
//
//            InetAddress clientAddress = data.getInetAddress();
//            int port = data.getPort();
//            logger.info("Connected to client: " + clientAddress.getHostAddress() + ":" + port);
//
//
//            String inputLine;
//
//            while ((!shouldStop) && (inputLine = in.readLine()) != null) {
//                logger.debug(String.format("Client[%b]: [%s]", shouldStop, inputLine));
//                try {
//                    logParser.putInQueue(inputLine);
//                } catch (InterruptedException e) {
//                    if (!shouldStop)
//                        logger.error("InterruptedException", e);
//                }
//            }
//
//        } catch (IOException e) {
//            if (!shouldStop)
//                logger.error("IOException", e);
//        } catch (Throwable e) {
//            logger.error("Throwable", e);
//        }
//        if (in != null) {
//            try {
//                in.close();
//                data.close();
//                logger.info("IN stream closed");
//            } catch (IOException e) {
//                logger.error("IOException", e);
//            }
//        }
//
//    }

    @Override
    public void run()
    {
        doRun = true;
        try
        {

            String clientIP = handlerSocket.getInetAddress().toString();
            int clientPort = handlerSocket.getPort();

            logger.info(String.format("Received a connection from %s:%s", clientIP, clientPort ));

            // Get input and output streams
            BufferedReader in = new BufferedReader( new InputStreamReader( handlerSocket.getInputStream() ) );
            PrintWriter out = new PrintWriter( handlerSocket.getOutputStream() );

            // Write out our header to the client
            //out.println( "GenericConnector" );
            //out.flush();

            // Echo lines back to the client until the client closes the connection or we receive an empty line
            String line;
            while ((doRun) && (line = in.readLine()) != null) {
                logParser.putInQueue(line);
                //out.println( "ok");
                //out.flush();
            }
            // Close our connection
            in.close();
            out.close();
            handlerSocket.close();

            logger.info(String.format("Connection closed from %s:%s", clientIP, clientPort ));
        }
        catch( SocketException e )
        {
            if (doRun)
                logger.error("SocketException", e);
        }
        catch( InterruptedException e )
        {
            if (doRun)
                logger.error("InterruptedException", e);
        }
        catch( Exception e )
        {
            logger.error("InterruptedException", e);
        }
    }

}
