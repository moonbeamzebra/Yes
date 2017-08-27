package ca.magenta.yes.api;

import ca.magenta.utils.AbstractTCPServer;
import ca.magenta.utils.AbstractTCPServerHandler;
import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.data.MasterIndex;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

public class APIServer extends AbstractTCPServerHandler {

    private static Logger logger = Logger.getLogger(APIServer.class);

    private static final String CLIENT_LABEL = "Client: ";

    private final MasterIndex masterIndex;

    APIServer(AbstractTCPServer tcpServer, String name, Socket handlerSocket, MasterIndex masterIndex) {

        super(tcpServer, name, handlerSocket);

        this.masterIndex = masterIndex;
    }

    @Override
    public void run()
    {
        setDoRun(true);
        try
        {

            String clientIP = handlerSocket.getInetAddress().toString();
            int clientPort = handlerSocket.getPort();

            logger.info(String.format("Received a connection from %s:%s", clientIP, clientPort ));

            BufferedReader in = new BufferedReader( new InputStreamReader( handlerSocket.getInputStream() ) );
            PrintWriter out = new PrintWriter(handlerSocket.getOutputStream(), true);

            String inputLine;

            if ((isDoRun()) && (inputLine = in.readLine()) != null) {
                logger.info(CLIENT_LABEL + inputLine);
                Control control = new Control(inputLine);

                Control.YesQueryMode mode = control.getMode();
                logger.info("Mode: " + mode);

                if (mode == Control.YesQueryMode.REAL_TIME) {

                    processRealTimeMode(in, out, control);

                } else if (mode == Control.YesQueryMode.LONG_TERM) {

                    processLongTermMode(in, out, control);

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
            if (isDoRun())
                logger.error(e.getClass().getSimpleName(), e);
        }
        catch( IOException | AppException e )
        {
            logger.error(e.getClass().getSimpleName(), e);
        }
    }

    private void processLongTermMode(BufferedReader in, PrintWriter out, Control control) throws AppException, IOException {

        String threadName = LongTermReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
        long olderTime = control.getLongTermReaderParams().getTimeRange().getOlderTime();
        logger.info("olderTimeStr: " + olderTime);
        long newerTime = control.getLongTermReaderParams().getTimeRange().getNewerTime();
        logger.info("newerTimeStr: " + newerTime);

        TimeRange periodTimeRange = new TimeRange(olderTime, newerTime);

        String searchString = control.getSearchString();
        logger.info("searchString: " + searchString);
        String partition = control.getPartition();
        if (partition != null)
            logger.info("partition: " + partition);

        int limit = control.getLongTermReaderParams().getLimit();
        if (limit > 0)
            logger.info("limit: " + limit);
        else if (limit <= 0)
            logger.info("limit: NO LIMIT");



        boolean reverse = control.getLongTermReaderParams().isReverse();
        logger.info("reverse: " + reverse);

        LongTermReader longTermReader = new LongTermReader(threadName,
                masterIndex,
                partition,
                searchString,
                new LongTermReader.Params(periodTimeRange, reverse, limit),
                out);


            longTermReader.startInstance();

        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            logger.info(CLIENT_LABEL + inputLine);
        }

        longTermReader.stopInstance();

    }

    private void processRealTimeMode(BufferedReader in, PrintWriter out, Control control) throws AppException, IOException {

        String threadName = RealTimeReader.class.getSimpleName() + "-" + tcpServer.getClientCount();
        String searchString = control.getSearchString();
        logger.info("searchString: " + searchString);

        RealTimeReader realTimeReader = new RealTimeReader(threadName, searchString, out);

        realTimeReader.startInstance();

        String inputLine;
        while ((isDoRun()) && (inputLine = in.readLine()) != null) {
            logger.info(CLIENT_LABEL + inputLine);

        }

        realTimeReader.stopInstance();

    }
}
