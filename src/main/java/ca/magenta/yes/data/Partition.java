package ca.magenta.yes.data;

import ca.magenta.utils.AppException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Partition extends AbsPartition {

    private String instanceName = null;

    public static Partition fromJSon(ObjectMapper mapper, String jsonMsg) throws AppException {
        try {
            return mapper.readValue(jsonMsg, Partition.class);
        } catch (IOException e) {
            throw new AppException(e.getClass().getSimpleName(),e);
        }
    }

    public String getInstanceName()
    {
        if (instanceName == null)
        {
            instanceName = (new StringBuilder()).append(this.getName()).append('-').append(getInstance()).toString();
        }

        return instanceName;
    }

}
