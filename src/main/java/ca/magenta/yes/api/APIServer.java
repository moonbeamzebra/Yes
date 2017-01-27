package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.utils.TCPServer;
import ca.magenta.utils.TimeRange;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;

/**
 * @author jean-paul.laberge <jplaberge@magenta.ca>
 * @version 0.1
 * @since 2014-11-25
 */
public class APIServer extends AbstractTCPServerHandler {

    private static Logger logger = Logger.getLogger(APIServer.class);

    private final String indexBaseDirectory;

    public APIServer(AbstractTCPServer tcpServer, String name, Socket handlerSocket, String indexBaseDirectory) {

        super(tcpServer, name, handlerSocket);

        this.indexBaseDirectory = indexBaseDirectory;
    }

//    public void runOld(Socket data) {
//        try {
//
//            String apiServerName = this.getClass().getSimpleName() + "-" + tcpServer.getClientCount();
//
//            InetAddress clientAddress = data.getInetAddress();
//            int port = data.getPort();
//            logger.info(apiServerName + " is now connected to client: " + clientAddress.getHostAddress() + ":" + port);
//
//            PrintWriter toClient = new PrintWriter(data.getOutputStream(), true);
//
//            BufferedReader fromClient = new BufferedReader(new InputStreamReader(data.getInputStream()));
//
//            String inputLine;
//
//            String mode = null;
//
//            String searchString = "*";
//            HashMap<String, String> control = null;
//            if ((inputLine = fromClient.readLine()) != null) {
//                logger.info("Client: " + inputLine);
//                ObjectMapper mapper = new ObjectMapper();
//                control = mapper.readValue(inputLine, HashMap.class);
//
//                mode = control.get("mode");
//                logger.info("Mode: " + mode);
//
//                if ("realTime".equals(mode)) {
//                    String threadName = RealTimeReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
//                    searchString = control.get("searchString");
//                    logger.info("searchString: " + searchString);
//
//                    RealTimeReader realTimeReader = new RealTimeReader(threadName, searchString, toClient);
//
//                    realTimeReader.startInstance();
//
//                    while ((!shouldStop) && (inputLine = fromClient.readLine()) != null) {
//                        logger.info("Client: " + inputLine);
//
//                    }
//
//                    realTimeReader.stopInstance();
//
//                } else if ("longTerm".equals(mode)) {
//                    String threadName = LongTermReader.class.getSimpleName() + "-" + this.getClientCount();
//                    String olderTimeStr = control.get("olderTime");
//                    logger.info("olderTimeStr: " + olderTimeStr);
//                    String newerTimeStr = control.get("newerTime");
//                    logger.info("newerTimeStr: " + newerTimeStr);
//
//                    TimeRange periodTimeRange = new TimeRange(Long.valueOf(olderTimeStr), Long.valueOf(newerTimeStr));
//
//                    searchString = control.get("searchString");
//                    logger.info("searchString: " + searchString);
//
//                    LongTermReader longTermReader = new LongTermReader(threadName,
//                            indexBaseDirectory,
//                            periodTimeRange,
//                            searchString,
//                            toClient);
//
//                    longTermReader.startInstance();
//
//                    while ((inputLine = fromClient.readLine()) != null) {
//                        logger.info("Client: " + inputLine);
//
//                    }
//
//                    longTermReader.stopInstance();
//                }
//            }
//            logger.info(apiServerName + " is now disconnected from client: " + clientAddress.getHostAddress() + ":" + port);
//
//            fromClient.close();
//            toClient.close();
//            data.close();
//            setClientCount(getClientCount() - 1);
//
//            // Process the data socket here.
//        } catch (SocketException e) {
//            if (!shouldStop)
//                logger.error("SocketException", e);
//        } catch (Exception e) {
//            logger.error("Exception", e);
//        }
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

            BufferedReader in = new BufferedReader( new InputStreamReader( handlerSocket.getInputStream() ) );
            PrintWriter out = new PrintWriter(handlerSocket.getOutputStream(), true);

            String inputLine;

            String mode = null;

            String searchString = "*";
            HashMap<String, String> control = null;
            if ((doRun) && (inputLine = in.readLine()) != null) {
                logger.info("Client: " + inputLine);
                ObjectMapper mapper = new ObjectMapper();
                control = mapper.readValue(inputLine, HashMap.class);

                mode = control.get("mode");
                logger.info("Mode: " + mode);

                if ("realTime".equals(mode)) {
                    String threadName = RealTimeReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
                    searchString = control.get("searchString");
                    logger.info("searchString: " + searchString);

                    RealTimeReader realTimeReader = new RealTimeReader(threadName, searchString, out);

                    realTimeReader.startInstance();

                    while ((doRun) && (inputLine = in.readLine()) != null) {
                        logger.info("Client: " + inputLine);

                    }

                    realTimeReader.stopInstance();

                } else if ("longTerm".equals(mode)) {
                    String threadName = LongTermReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
                    String olderTimeStr = control.get("olderTime");
                    logger.info("olderTimeStr: " + olderTimeStr);
                    String newerTimeStr = control.get("newerTime");
                    logger.info("newerTimeStr: " + newerTimeStr);

                    TimeRange periodTimeRange = new TimeRange(Long.valueOf(olderTimeStr), Long.valueOf(newerTimeStr));

                    searchString = control.get("searchString");
                    logger.info("searchString: " + searchString);

                    LongTermReader longTermReader = new LongTermReader(threadName,
                            indexBaseDirectory,
                            periodTimeRange,
                            searchString,
                            out);

                    longTermReader.startInstance();

                    while ((inputLine = in.readLine()) != null) {
                        logger.info("Client: " + inputLine);

                    }

                    longTermReader.stopInstance();
                }
            }

            in.close();
            out.close();
            handlerSocket.close();
            tcpServer.setClientCount(tcpServer.getClientCount() - 1);

            logger.info(String.format("Connection closed from %s:%s", clientIP, clientPort ));
        }
        catch( SocketException e )
        {
            if (doRun)
                logger.error("SocketException", e);
        }
        catch( Exception e )
        {
            logger.error("InterruptedException", e);
        }
    }

//    public void runOrg()
//    {
//        doRun = true;
//        try
//        {
//
//            String clientIP = handlerSocket.getInetAddress().toString();
//            int clientPort = handlerSocket.getPort();
//
//            logger.info(String.format("Received a connection from %s:%s", clientIP, clientPort ));
//
//            BufferedReader in = new BufferedReader( new InputStreamReader( handlerSocket.getInputStream() ) );
//            PrintWriter out = new PrintWriter( handlerSocket.getOutputStream() );
//
//            String line;
//            while ((doRun) && (line = in.readLine()) != null) {
//                logParser.putInQueue(line);
//                //out.println( "ok");
//                //out.flush();
//            }
//            // Close our connection
//            in.close();
//            out.close();
//            handlerSocket.close();
//            tcpServer.setClientCount(tcpServer.getClientCount() - 1);
//
//            logger.info(String.format("Connection closed from %s:%s", clientIP, clientPort ));
//        }
//        catch( SocketException e )
//        {
//            if (doRun)
//                logger.error("SocketException", e);
//        }
//        catch( InterruptedException e )
//        {
//            if (doRun)
//                logger.error("InterruptedException", e);
//        }
//        catch( Exception e )
//        {
//            logger.error("InterruptedException", e);
//        }
//    }

}
