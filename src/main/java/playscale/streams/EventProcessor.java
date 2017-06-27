package playscale.streams;


import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Predicate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import java.util.HashMap;
import java.util.HashSet;

/**
 * This class is responsible for taking in events as they are consumed by Kafka Streams
 * from the "event" topic, and turned into a JSON string containing a set of "signals".
 * This class implements the Predicate interface to take advantage of Java 8 lambda syntax.
 * In the EventToSignal stream class, by passing an instance of this class to the filter method,
 * "test" is invoked automatically with the key and value passed in.
 */

public class EventProcessor implements Predicate<Long,GenericRecord> {

    private HashMap<Long,HashSet<String>> userCountries;
    private HashMap<Long,HashSet<Long>> userDevices;

    public EventProcessor() {
        userCountries = new HashMap<>();
        userDevices = new HashMap<>();
    }

    @Override
    public boolean test(Long key, GenericRecord value) {
        Long userId = (Long) value.get("user_id");
        if (userId == null) return false; // if userId is null, this is an "anonymous" event and should be handled differently
        return true; // allow this event to have signals extracted
    }

    // process an event record into a signal record
    public KeyValue<Long,JSONObject> extractSignals(Long key, GenericRecord value) {

        Long userId = (Long) value.get("user_id");
        Long deviceId = (Long) value.get("device_id");
        String country = getCountry(value);

        JSONObject signalsObject = new JSONObject();
        JSONArray signalsArray = new JSONArray();

        signalsObject.put("created_at",value.get("created_at"));
        signalsObject.put("tenant_id", key);
        signalsObject.put("user_id",userId);
        signalsObject.put("device_id",deviceId);
        signalsObject.put("session_id",value.get("session_id"));

        // create new country signal
        if(isNewCountry(userId,country)) {
            JSONObject newCountry = new JSONObject();
            newCountry.put("new_country",country);
            signalsArray.add(newCountry);
        }

        // create new device signal
        if(isNewDevice(userId,deviceId)) {
            // create new device signal
            JSONObject newDevice = new JSONObject();
            newDevice.put("new_device",deviceId);
            signalsArray.add(newDevice);
        }

        // Create ip_tag signal
        JSONArray ipTags = getIpTags(value);
        if(ipTags != null) {
            JSONObject tags = new JSONObject();
            tags.put("ip_tags",ipTags);
            signalsArray.add(tags);
        }

        signalsObject.put("signals",signalsArray);
        return new KeyValue<>(key,signalsObject);
    }

    // extract a set of iptags from the event record
    private JSONArray getIpTags(GenericRecord record) {
        GenericRecord requestContext = (GenericRecord) record.get("request_context");
        GenericData.Array<GenericData.EnumSymbol> tagArray =
                (GenericData.Array<GenericData.EnumSymbol>) requestContext.get("ip_tags");

        // if there are no ip_tags
        if (tagArray.size() < 1) return null;

        JSONArray ipTags = new JSONArray();
        for(int i = 0; i < tagArray.size(); i++) {
            ipTags.add(tagArray.get(i).toString());
        }

        return ipTags;
    }

    // extract the country iso code from the event record
    private String getCountry(GenericRecord record) {
        GenericRecord requestContext = (GenericRecord) record.get("request_context");
        GenericRecord ipData = (GenericRecord) requestContext.get("ip_data");
        Object isoCode = ipData.get("country_iso_code");
        return isoCode != null ? isoCode.toString() : null;
    }

    // check if the country io code is listed in the user's set of known countries
    private boolean isNewCountry(Long userId, String country) {

        if (country == null) return false;

        HashSet<String> countries;
        if (userCountries.containsKey(userId)) {
            countries = userCountries.get(userId);
            if (!countries.contains(country)) {
                countries.add(country);
                return true;
            }

            return false;
        }

        countries = new HashSet<>();
        countries.add(country);
        userCountries.put(userId,countries);
        return false;
    }

    // check if the device is listed in the user's set of known devices
    private boolean isNewDevice(Long userId, Long deviceId) {

        if (deviceId == null) return false;

        HashSet<Long> devices;
        if (userDevices.containsKey(userId)) {
            devices = userDevices.get(userId);
            if (!devices.contains(deviceId)) {
                devices.add(deviceId);
                return true;
            }

            return false;
        }

        devices = new HashSet<>();
        devices.add(deviceId);
        userDevices.put(userId,devices);
        return false;
    }

}
