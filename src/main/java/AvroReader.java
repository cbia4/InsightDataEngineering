import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class AvroReader {

    // Singleton
    private AvroReader() {}

    public static List<String> deserialize(InputStream inputStream) throws IOException {

        List<String> eventList = new ArrayList<>();
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/event_schema.avsc"));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream,datumReader);

        GenericRecord event = null;
        while(dataFileReader.hasNext()) {
            try {
                event = dataFileReader.next(event);
                eventList.add(event.toString());
            } catch (EOFException e) {
                break;
            }
        }

        return eventList;
    }
}
