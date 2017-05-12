package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.data.MasterIndex;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
    private final MasterIndex masterIndex;

    public APIServer(AbstractTCPServer tcpServer, String name, Socket handlerSocket, String indexBaseDirectory, MasterIndex masterIndex) {

        super(tcpServer, name, handlerSocket);

        this.indexBaseDirectory = indexBaseDirectory;
        this.masterIndex = masterIndex;
    }

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

            Control.YesQueryMode mode = null;

            String partition = null;
            String searchString = "*";
            //HashMap<String, String> control = null;
            Control control = null;
            if ((doRun) && (inputLine = in.readLine()) != null) {
                logger.info("Client: " + inputLine);
                ObjectMapper mapper = new ObjectMapper();
                control = mapper.readValue(inputLine, Control.class);

                mode = control.getMode();
                logger.info("Mode: " + mode);

                if (mode == Control.YesQueryMode.REAL_TIME) {
                    String threadName = RealTimeReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
                    searchString = control.getSearchString();
                    logger.info("searchString: " + searchString);

                    RealTimeReader realTimeReader = new RealTimeReader(threadName, searchString, out);

                    realTimeReader.startInstance();

                    while ((doRun) && (inputLine = in.readLine()) != null) {
                        logger.info("Client: " + inputLine);

                    }

                    realTimeReader.stopInstance();

                } else if (mode == Control.YesQueryMode.LONG_TERM) {
                    String threadName = LongTermReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
                    long olderTime = control.getOlderTime();
                    logger.info("olderTimeStr: " + olderTime);
                    long newerTime = control.getNewerTime();
                    logger.info("newerTimeStr: " + newerTime);

                    TimeRange periodTimeRange = new TimeRange(olderTime, newerTime);

                    searchString = control.getSearchString();
                    logger.info("searchString: " + searchString);
                    partition = control.getPartition();
                    if (partition != null)
                        logger.info("partition: " + partition);

                    int limit = control.getLimit();
                    if (limit > 0)
                        logger.info("limit: " + limit);
                    else if (limit <= 0)
                        logger.info("limit: NO LIMIT");



                    boolean reverse = control.isReverse();
                    logger.info("reverse: " + reverse);

                    LongTermReader longTermReader = new LongTermReader(threadName,
                            indexBaseDirectory,
                            periodTimeRange,
                            partition,
                            limit,
                            searchString,
                            masterIndex,
                            reverse,
                            out);

                    longTermReader.startInstance();

                    while ((inputLine = in.readLine()) != null) {
                        logger.info("Client: " + inputLine);
                    }

                    longTermReader.gentleStopIt();

                    longTermReader.stopInstance();
                }
            }

            in.close();
            out.close();
            handlerSocket.close();
            tcpServer.removeTcpServerHandler(this);

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
}
