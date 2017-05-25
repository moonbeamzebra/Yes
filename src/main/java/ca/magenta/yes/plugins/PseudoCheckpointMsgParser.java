package ca.magenta.yes.plugins;

import ca.magenta.utils.AppException;
import ca.magenta.yes.connector.EffectiveMsgParser;
import ca.magenta.yes.data.ParsedMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PseudoCheckpointMsgParser implements EffectiveMsgParser {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static final String TYPE = "pseudoCheckpoint";

    @Override
    public ParsedMessage doParse(ObjectMapper mapper, String message) throws AppException {

        ParsedMessage parsedMessage = new ParsedMessage(TYPE);

        // Let's parse pseudo Checkpoint logs
        // device=fw01|source=10.10.10.10|dest=20.20.20.20|port=80|action=drop
        String[] items = message.split("\\|");

        for (String item : items) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("ITEM:[%s]", item));
            int pos = item.indexOf("=");
            if (pos != -1) {
                String key = item.substring(0, pos);
                String value = item.substring(pos + 1);

                if (logger.isDebugEnabled())
                    logger.debug(String.format("KEY:[%s]; VALUE:[%s]", key, value));

                parsedMessage.putKeyValuPair(key, value);

            }
        }

        return parsedMessage;
    }

}
