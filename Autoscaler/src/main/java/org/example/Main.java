package org.example;

import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.JsonNode;
import io.joshworks.restclient.http.Unirest;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


public class Main{

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Thread.sleep(180000);
        Scaler scaler = new Scaler();
        scaler.start();

}

}
