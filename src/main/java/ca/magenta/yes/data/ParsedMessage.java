package ca.magenta.yes.data;

import java.util.HashMap;

public class ParsedMessage {

    private final String type;


    private long srcTimestamp;
    private final HashMap<String, Object> messageHashed = new HashMap<>();

    public ParsedMessage(String type) {
        this.type = type;
    }

    public void setSrcTimestamp(long srcTimestamp) {
        this.srcTimestamp = srcTimestamp;
    }

    public Object putKeyValuPair(String key, Object obj) {
        return messageHashed.put(key, obj);
    }

    private long getSrcTimestamp() {
        return srcTimestamp;
    }

    String getType() {
        return type;
    }

    HashMap<String, Object> getMessageHashed() {
        return messageHashed;
    }


}
