package ca.dealsaccess.holt.log;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AbstractLogEntrySerializer extends Serializer<ApacheLogEntry> {
	@Override
    public void write(Kryo kryo, Output output, ApacheLogEntry logEntry) {
        output.writeLong(logEntry.getTimeStamp());
    }

    @SuppressWarnings("rawtypes")
	@Override
    public ApacheLogEntry read(Kryo kryo, Input input, Class type) {
        long l = input.readLong();
        ApacheLogEntry logEntry = new ApacheLogEntry();
        logEntry.timestamp = l;
        return logEntry;
    }    
}
