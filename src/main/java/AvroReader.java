import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class AvroReader {

    // Singleton
    private AvroReader() {}

    public static void run() {

        // Create some users and set their fields
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        // Leave favorite color null

        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        // Serialize user1, user2, and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            dataFileWriter.create(user1.getSchema(), new File("../resources/users.avro"));
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
            dataFileWriter.close();
        } catch (IOException e) {
            System.err.println("File Creation Failed. Exiting.");
            System.exit(-1);
        }
    }
}
