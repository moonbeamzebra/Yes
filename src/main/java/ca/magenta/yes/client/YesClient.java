package ca.magenta.yes.client;

import ca.magenta.utils.TimeRange;
import ca.magenta.yes.api.LongTermReader;
import ca.magenta.yes.data.NormalizedLogRecord;
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


    public void showLongTermEntries(TimeRange periodTimeRange, String searchString, OutputOption outputOption) {
        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            String control = String.format("{\"mode\":\"longTerm\",\"olderTime\":\"%s\",\"newerTime\":\"%s\",\"searchString\":\"%s\"}",
                    periodTimeRange.getOlderTime(),
                    periodTimeRange.getNewerTime(),
                    searchString);

            toServer.println(control);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;
            boolean doRun = true;

            String lastMessage = "";
            while (doRun && (entry = fromServer.readLine()) != null) {
                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
                    YesClient.printEntry(NormalizedLogRecord.fromJson(entry), outputOption);
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
    }

    public List<NormalizedLogRecord> findAll(TimeRange periodTimeRange, String searchString) {

        List<NormalizedLogRecord> list = new ArrayList<>();


        try {

            Socket apiServer = new Socket(apiServerAddr, apiServerPort);
            PrintWriter toServer = new PrintWriter(apiServer.getOutputStream(), true);

            String control = String.format("{\"mode\":\"longTerm\",\"olderTime\":\"%s\",\"newerTime\":\"%s\",\"searchString\":\"%s\"}",
                    periodTimeRange.getOlderTime(),
                    periodTimeRange.getNewerTime(),
                    searchString);

            toServer.println(control);

            BufferedReader fromServer = new BufferedReader(new InputStreamReader(apiServer.getInputStream()));

            String entry;
            boolean doRun = true;

            String lastMessage = "";
            while (doRun && (entry = fromServer.readLine()) != null) {
                if (!(entry.startsWith(LongTermReader.END_DATA_STRING))) {
                    list.add(NormalizedLogRecord.fromJson(entry));
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


    public static void printEntry(NormalizedLogRecord normalizedLogRecord, OutputOption outputOption) throws IOException {

        // DEFAULT, RAW, JSON, TWO_LINER
        switch (outputOption) {
            case JSON:
                System.out.println(normalizedLogRecord.toJson());
                break;
            case RAW:
                System.out.println(String.format("[%s][%s] %s",
                        normalizedLogRecord.getPrettyRxTimestamp(),
                        normalizedLogRecord.getPartition(),
                        normalizedLogRecord.getMessage()));
                break;
            case TWO_LINER:
                System.out.println(normalizedLogRecord.toTTYString(true, false));
                break;

            case DEFAULT:
                System.out.println(normalizedLogRecord.toTTYString(false, true));
        }
    }

    public enum OutputOption {
        DEFAULT, RAW, JSON, TWO_LINER
    }


}
