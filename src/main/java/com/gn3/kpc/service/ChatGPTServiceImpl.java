package com.gn3.kpc.service;


import com.github.tuguri8.lib.KoreanSummarizer;
import com.google.common.base.Charsets;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

public class ChatGPTServiceImpl implements ChatGPTService, Serializable{
    public String chatGPT(String prompt, String question) {
        KoreanSummarizer koreanSummarizer = new KoreanSummarizer();
        prompt = koreanSummarizer.summarize(prompt.replaceAll("  ", ""));

        prompt += (question);
        String url = "https://api.openai.com/v1/chat/completions";
        String apiKey = "";
        String model = "gpt-3.5-turbo";
        String result = "";
        try {

            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader("Authorization", "Bearer " + apiKey);
            httpPost.addHeader("Content-Type", "application/json");

            JSONParser jsonParser = new JSONParser();
            Object obj = jsonParser.parse("{\"model\": \"" + model + "\", \"messages\": [{\"role\": \"user\", \"content\": \"" + prompt + "\"}]}");
            JSONObject requestData = (JSONObject) obj;
            requestData.put("model", "gpt-3.5-turbo");
            requestData.put("temperature", 0.7);
            requestData.put("max_tokens", 500);

            StringEntity requestEntity = new StringEntity(requestData.toString(), HTTP.UTF_8);
            httpPost.setEntity(requestEntity);
            CloseableHttpResponse response = httpClient.execute(httpPost);

            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null) {
                String responseBody = EntityUtils.toString(responseEntity, Charsets.UTF_8);
                org.json.JSONObject jsonResponse = new org.json.JSONObject(responseBody);
                result = jsonResponse.getJSONArray("choices").getJSONObject(0).getJSONObject("message").getString("content");
            }

            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public String extractMessageFromJSONResponse(String response) {
        int start = response.indexOf("content")+ 11;
        int end = response.indexOf("\"", start);
        return response.substring(start, end);
    }
}
