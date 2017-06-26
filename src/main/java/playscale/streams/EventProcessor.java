package playscale.streams;


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Predicate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import java.util.HashMap;
import java.util.HashSet;

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

        if(isNewCountry(userId,country)) {
            // create new country signal
            JSONObject newCountry = new JSONObject();
            newCountry.put("new_country",country);
            signalsArray.add(newCountry);
        }

        if(isNewDevice(userId,deviceId)) {
            // create new device signal
            JSONObject newDevice = new JSONObject();
            newDevice.put("new_device",deviceId);
            signalsArray.add(newDevice);
        }

        signalsObject.put("signals",signalsArray);
        return new KeyValue<>(key,signalsObject);
    }

    private String getCountry(GenericRecord record) {
        GenericRecord requestContext = (GenericRecord) record.get("request_context");
        GenericRecord ipData = (GenericRecord) requestContext.get("ip_data");
        Object isoCode = ipData.get("country_iso_code");
        return isoCode != null ? isoCode.toString() : null;
    }

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
