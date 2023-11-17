package com.gn3.kpc.service;

public interface ChatGPTService {
    public String chatGPT(String prompt, String question);
    public String extractMessageFromJSONResponse(String response);
}
