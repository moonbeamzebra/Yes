package ca.magenta.yes.api;

import ca.magenta.utils.AppException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Control {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public Control(YesQueryMode mode,
                   String partition,
                   int limit,
                   long olderTime,
                   long newerTime,
                   String searchString,
                   boolean reverse) {
        this.mode = mode;
        this.partition = partition;
        this.limit = limit;
        this.olderTime = olderTime;
        this.newerTime = newerTime;
        this.searchString = searchString;
        this.reverse = reverse;
    }

    public Control(String jsonStr) throws AppException {

        try {
            Control data = (new ObjectMapper()).readValue(jsonStr, Control.class);

            this.mode = data.getMode();
            this.searchString = data.getSearchString();

            this.partition = data.getPartition();
            this.limit = data.getLimit();
            this.olderTime = data.getOlderTime();
            this.newerTime = data.getNewerTime();
            this.reverse = data.isReverse();

        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }

    }

    public Control(YesQueryMode mode, String searchString) {
        this.mode = mode;
        this.searchString = searchString;

        this.partition = null; // Not used in read time mode
        this.limit = 0; // Not used in read time mode
        this.olderTime = 0; // Not used in read time mode
        this.newerTime = 0; // Not used in read time mode
        this.reverse = false; // Not used in read time mode
    }

    private Control() {
    }

    public String toJson() throws AppException {

        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }
    }

    @Override
    public String toString() {
        String json =  "Control{" +
                "mode=" + mode +
                ", partition='" + partition + '\'' +
                ", limit=" + limit +
                ", olderTime=" + olderTime +
                ", newerTime=" + newerTime +
                ", searchString='" + searchString + '\'' +
                ", reverse=" + reverse +
                '}';
        try {
            return toJson();
        } catch (AppException e) {
            logger.error(e.getClass().getSimpleName(), e);
        }

        return json;
    }

    private YesQueryMode mode = YesQueryMode.LONG_TERM;
    private String partition = null;
    private int limit = 0;
    private long olderTime = 0;
    private long newerTime = 0;
    private String searchString = null;
    private boolean reverse = false;

    public YesQueryMode getMode() {
        return mode;
    }

    public String getPartition() {
        return partition;
    }

    public int getLimit() {
        return limit;
    }

    public long getOlderTime() {
        return olderTime;
    }

    public long getNewerTime() {
        return newerTime;
    }

    public String getSearchString() {
        return searchString;
    }

    public boolean isReverse() {
        return reverse;
    }

    public enum YesQueryMode {
        LONG_TERM, REAL_TIME
    }

}
