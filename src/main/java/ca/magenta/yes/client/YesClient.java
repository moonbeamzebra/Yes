package ca.magenta.yes.client;

import ca.magenta.utils.AppException;
import ca.magenta.utils.TimeRange;
import ca.magenta.yes.Yes;
import ca.magenta.yes.api.Control;
import ca.magenta.yes.api.LongTermReader;
import ca.magenta.yes.data.NormalizedMsgRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


public class YesClient {

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    private final String apiServerAddr;
    private final int apiServerPort;

    public YesClient(String apiServerAddr, int apiServerPort) {
        this.apiServerAddr = apiServerAddr;
        this.apiServerPort = apiServerPort;
    }


    public void showLongTermEntries(Yes.State state, String partition,
                                    int limit,
                                    TimeRange periodTimeRange,
                                    String searchString,
                                    boolean reverse,
                                    OutputOption outputOption) throws AppException {

        try {
            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            Control control = new Control(Control.YesQueryMode.LONG_TERM,
                    partition,
                    limit,
                    periodTimeRange.getOlderTime(),
                    periodTimeRange.getNewerTime(),
                    searchString,
                    reverse
                    );

            toServer.println(control.toJson());

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;

            ObjectMapper mapper = new ObjectMapper();
            String lastMessage = "";
            while (state.doRun && (entry = fromServer.readLine()) != null) {
                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
                    YesClient.printEntry(mapper,
                            new NormalizedMsgRecord(mapper, entry,false),
                            outputOption);
                } else {
                    lastMessage = entry;
                    state.doRun = false;
                }

            }

            toServer.close();
            fromServer.close();
            apiServer.close();

            if (lastMessage.length() > LongTermReader.END_DATA_STRING.length()) {
                logger.error(String.format("%s", lastMessage));
            }
        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }

    }

    public void showRealTimeEntries(Yes.State state, String searchString, OutputOption outputOption) throws AppException {
        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            Control control = new Control(Control.YesQueryMode.REAL_TIME,
                    searchString
            );

            toServer.println(control.toJson());

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;

            ObjectMapper mapper = new ObjectMapper();
            while (state.doRun && (entry = fromServer.readLine()) != null) {
                YesClient.printEntry(mapper,
                        new NormalizedMsgRecord(mapper, entry,false),
                        outputOption);

            }

            fromServer.close();
            apiServer.close();
        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }
    }


    public List<NormalizedMsgRecord> findAll(TimeRange periodTimeRange, String searchString, boolean reverse) {

        List<NormalizedMsgRecord> list = new ArrayList<>();


        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            Control control = new Control(Control.YesQueryMode.LONG_TERM,
                    null,
                    9,
                    periodTimeRange.getOlderTime(),
                    periodTimeRange.getNewerTime(),
                    searchString,
                    reverse
            );


//            String control = String.format(
//                    "{\"mode\":\"longTerm\",\"olderTime\":\"%s\",\"newerTime\":\"%s\",\"searchString\":\"%s\",\"reverse\":\"%s\"}",
//                    periodTimeRange.getOlderTime(),
//                    periodTimeRange.getNewerTime(),
//                    searchString,
//                    Boolean.toString(reverse)
//            );

            toServer.println(control.toJson());

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;
            boolean doRun = true;

            ObjectMapper mapper = new ObjectMapper();
            String lastMessage = "";
            while (doRun && (entry = fromServer.readLine()) != null) {
                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
                    list.add(new NormalizedMsgRecord(mapper,entry,false));
                } else {
                    lastMessage = entry;
                    doRun = false;
                }

            }

            fromServer.close();
            apiServer.close();

            if (lastMessage.length() > LongTermReader.END_DATA_STRING.length()) {
                logger.error(String.format("%s", lastMessage));
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }

        return list;
    }


    public static void printEntry(ObjectMapper mapper, NormalizedMsgRecord normalizedLogRecord, OutputOption outputOption) throws IOException {

        // DEFAULT, SIMPLE, JSON, TWO_LINER
        switch (outputOption) {
            case JSON:
                System.out.println(normalizedLogRecord.toJson(mapper));
                break;
            case RAW:
                System.out.println(normalizedLogRecord.getMessage());
                break;
            case SIMPLE:
                System.out.println(String.format("[%s][%s] %s",
                        normalizedLogRecord.getPrettyRxTimestamp(),
                        normalizedLogRecord.getPartition(),
                        normalizedLogRecord.getMessage()));
                break;
            case TWO_LINER:
                System.out.println(normalizedLogRecord.toRawString(true, false));
                break;

            case DEFAULT:
                System.out.println(normalizedLogRecord.toRawString(false, true));
        }
    }


    public enum OutputOption {
        DEFAULT, RAW, SIMPLE, JSON, TWO_LINER
    }


}
