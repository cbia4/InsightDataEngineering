package playscale.extractors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Predicate;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;


public class CountryStore implements Predicate<Long,GenericRecord> {

    private HashMap<Long,HashSet<String>> userCountryStore;
    private Schema signalSchema;

    public CountryStore(String path) throws IOException {
        this.userCountryStore = new HashMap<>();
        this.signalSchema = new Schema.Parser().parse(new File(path));
    }

    // Extract the country_iso_code from a generic record
    private String getIsoCode(GenericRecord record) {
        GenericRecord requestContext = (GenericRecord) record.get("request_context");
        GenericRecord ipData = (GenericRecord) requestContext.get("ip_data");
        Object isoCode = ipData.get("country_iso_code");
        if(isoCode != null) return isoCode.toString();
        return null;
    }

    @Override
    public boolean test(Long key, GenericRecord record) {

        Long userId = (Long) record.get("user_id"); // Use object type - user_id may be null
        String countryIsoCode = getIsoCode(record);

        // Ignore if either field is null
        if(userId == null || countryIsoCode == null) return false;

        if(userCountryStore.containsKey(userId)) {
            if(!userCountryStore.get(userId).contains(countryIsoCode)) {
                userCountryStore.get(userId).add(countryIsoCode);
                return true; // let this event go through, new country signal necessary
            }

            return false; // user and country have already been registered - not a new country
        }

        userCountryStore.put(userId, new HashSet<>());
        userCountryStore.get(userId).add(countryIsoCode);

        return false; // user had not previously been registered in the store

    }

    public GenericRecord createNewCountrySignal(GenericRecord event) {
        GenericRecord newCountrySignal = new GenericData.Record(this.signalSchema);
        newCountrySignal.put("created_at", event.get("created_at"));
        newCountrySignal.put("user_id", event.get("user_id"));
        newCountrySignal.put("tenant_id", event.get("tenant_id"));
        newCountrySignal.put("device_id", event.get("device_id"));
        newCountrySignal.put("session_id", event.get("session_id"));
        newCountrySignal.put("country_iso_code",event.get("country_iso_code"));
        return newCountrySignal;
    }

}
