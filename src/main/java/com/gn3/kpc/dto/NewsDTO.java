package com.gn3.kpc.dto;

import lombok.Data;
import org.json.simple.JSONObject;

import java.io.Serializable;

@Data
public class NewsDTO implements DTO, Serializable {
    private String title;
    private String content;
    private String joinDate;

    @Override
    public JSONObject DTOInfo() {
        JSONObject dtoInfo = new JSONObject();
        dtoInfo.put("title", title);
        dtoInfo.put("content", content);
        dtoInfo.put("joinDate", joinDate);
        return dtoInfo;
    }
}
