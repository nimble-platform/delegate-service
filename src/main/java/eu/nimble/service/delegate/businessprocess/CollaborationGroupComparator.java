package eu.nimble.service.delegate.businessprocess;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class CollaborationGroupComparator implements Comparator<JsonObject> {
    DateTimeFormatter bpFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    public int compare(JsonObject str1, JsonObject str2)
    {
        LocalDateTime first_Str;
        LocalDateTime second_Str;
        first_Str = getLastActivityTimeForCollaboration(str1);
        second_Str = getLastActivityTimeForCollaboration(str2);
        return second_Str.compareTo(first_Str);
    }

    private LocalDateTime getLastActivityTimeForCollaboration(JsonObject jsonObject){
//        try {
        JsonArray processInstanceGroups = jsonObject.get("associatedProcessInstanceGroups").getAsJsonArray();
        int size = processInstanceGroups.size();
        String lastActivityTimeString = processInstanceGroups.get(0).getAsJsonObject().get("lastActivityTime").getAsString();
        LocalDateTime lastActivityTime = bpFormatter.parseLocalDateTime(lastActivityTimeString);

        for(int i = 0; i < size; i++){
            LocalDateTime time = bpFormatter.parseLocalDateTime(processInstanceGroups.get(i).getAsJsonObject().get("lastActivityTime").getAsString());
            if(time.isAfter(lastActivityTime)){
                lastActivityTime = time;
            }
        }
        return lastActivityTime;
//        }
//        catch (Exception e){
//            throw new RuntimeException("Failed to compare process instance groups");
//        }
    }
}
