package ca.magenta.yes.connector;

import ca.magenta.utils.AppException;
import ca.magenta.yes.data.ParsedMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultEffectiveMsgParser implements EffectiveMsgParser {

    @Override
    public ParsedMessage doParse(ObjectMapper mapper, String message) throws AppException {

        // Just don't parse at all
        return new ParsedMessage(EffectiveMsgParser.DEFAULT_LOG_TYPE);
    }
}
