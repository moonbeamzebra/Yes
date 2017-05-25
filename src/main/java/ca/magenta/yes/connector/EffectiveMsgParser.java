package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.yes.data.ParsedMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface EffectiveMsgParser {

    String DEFAULT_LOG_TYPE = "rawMessage";

    public ParsedMessage doParse(ObjectMapper mapper, String message) throws AppException;

}
