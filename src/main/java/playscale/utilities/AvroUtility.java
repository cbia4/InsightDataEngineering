package playscale.utilities;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
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
import java.util.Iterator;
import java.util.List;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class AvroUtility {

    private Schema schema;
    private Injection<GenericRecord,byte[]> recordInjection;

    public AvroUtility(String path) throws IOException {
        this.schema = new Schema.Parser().parse(new File(path));
        this.recordInjection = GenericAvroCodecs.toBinary(this.schema);
    }

    public byte[] encode(GenericRecord record) {
        return recordInjection.apply(record);
    }

    public GenericRecord decode(byte[] bytes) {
        return recordInjection.invert(bytes).get();
    }

    public Iterator<GenericRecord> getRecords(InputStream inputStream) throws IOException  {

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, datumReader);
        Iterator<GenericRecord> recordIterator = dataFileStream.iterator();
        return recordIterator;

    }

}
