package io.searchbox.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Chris on 19/02/15.
 */
public class SuggestResult extends JestResult {
    private String text;

    public SuggestResult(Gson gson) {
        super(gson);
    }

    public List<String> getSuggests(){
        JsonArray jsonOptions = extractJsonOptionsArray(jsonObject);

        return StreamSupport.stream(jsonOptions.spliterator(), false)
                .map((option) -> {
                    return option.getAsJsonObject().get("text").getAsString();
                })
                .collect(Collectors.toList());
    }

    private static JsonArray extractJsonOptionsArray(JsonObject jsonObject) {
        // one object array ...
        JsonArray completeJSONElement = jsonObject.getAsJsonArray("complete").getAsJsonArray();
        if(completeJSONElement == null ||completeJSONElement.size() == 0){
            return null;
        }

        return completeJSONElement.get(0).getAsJsonObject().getAsJsonArray("options");
    }

    public <T> List<SuggestWithPayLoad<T>> getSuggestsWithPayLoad(Class<T> clazz){
        JsonArray jsonOptions = extractJsonOptionsArray(jsonObject);

        return StreamSupport.stream(jsonOptions.spliterator(), false)
                .map((option) -> {
                    String value = option.getAsJsonObject().get("text").getAsString();
                    T payload = gson.fromJson(option.getAsJsonObject().get("payload"), clazz);

                    return new SuggestWithPayLoad<T>(
                            value,
                            payload);

                }).collect(Collectors.toList());
    }

    public static class SuggestWithPayLoad<T> {
        public final String suggest;
        public final T paylod;

        public SuggestWithPayLoad(String suggest, T paylod) {
            this.suggest = suggest;
            this.paylod = paylod;
        }
    }
}
