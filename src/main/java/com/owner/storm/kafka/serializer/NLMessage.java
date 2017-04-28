package com.owner.storm.kafka.serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by napo on 2017/4/28.
 */
public class NLMessage {
    private Map headers = new HashMap();
    private byte[] body;

    public Map getHeaders() {
        return headers;
    }

    public void setHeaders(Map headers) {
        this.headers = headers;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
