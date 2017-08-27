package ca.magenta.yes.api;

import ca.magenta.utils.AppException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Control {

    private static final Logger logger = LoggerFactory.getLogger(Control.class.getName());

    public Control(YesQueryMode mode,
                   String partition,
                   String searchString,
                   LongTermReader.Params longTermReaderParams)
 {
        this.mode = mode;
        this.partition = partition;
        this.searchString = searchString;
        this.longTermReaderParams = longTermReaderParams;
    }

    Control(String jsonStr) throws AppException {

        try {
            Control data = (new ObjectMapper()).readValue(jsonStr, Control.class);

            this.mode = data.getMode();
            this.partition = data.getPartition();
            this.searchString = data.getSearchString();

            this.longTermReaderParams = data.getLongTermReaderParams();

        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(), e);
        }

    }

    public Control(YesQueryMode mode, String searchString) {
        this.mode = mode;
        this.searchString = searchString;
        this.partition = null; // Not used in read time mode (yet)
    }

    // Required for Json mapping
    public Control() {
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
                ", searchString='" + searchString + '\'' +
                ", olderTime=" + longTermReaderParams.getTimeRange().getOlderTime() +
                ", newerTime=" + longTermReaderParams.getTimeRange().getNewerTime() +
                ", reverse=" + longTermReaderParams.isReverse() +
                ", limit=" + longTermReaderParams.getLimit() +
                '}';
        try {
            return toJson();
        } catch (AppException e) {
            logger.error(e.getClass().getSimpleName(), e);
        }

        return json;
    }

    private YesQueryMode mode = null;
    private String partition = null;
    private String searchString = null;

    private LongTermReader.Params longTermReaderParams = null;

    // Required public for Json mapping
    public YesQueryMode getMode() {
        return mode;
    }

    public LongTermReader.Params getLongTermReaderParams() {
        return longTermReaderParams;
    }

    public String getPartition() {
        return partition;
    }

    public String getSearchString() {
        return searchString;
    }

    public enum YesQueryMode {
        LONG_TERM, REAL_TIME
    }

}
