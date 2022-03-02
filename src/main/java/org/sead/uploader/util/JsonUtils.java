/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.util;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonWriter;
import java.io.StringReader;
import java.io.StringWriter;



public class JsonUtils {

    static public String stringify(JsonStructure jsonObjectOrArray) {
        StringWriter stWriter = new StringWriter();
        JsonWriter jsonWriter = Json.createWriter(stWriter);
        jsonWriter.write(jsonObjectOrArray);
        jsonWriter.close();
        String jsonString = stWriter.toString();
        return jsonString;
    }

    static public JsonObject parse(String jsonObjectString) {
        JsonReader jsonReader = Json.createReader(new StringReader(jsonObjectString));
        JsonObject jsonObject = jsonReader.readObject();
        return jsonObject;
    }

    static public JsonArray parseArray(String jsonArrayString) {
        JsonReader jsonReader = Json.createReader(new StringReader(jsonArrayString));
        JsonArray jsonArray = jsonReader.readArray();
        return jsonArray;
    }
}
