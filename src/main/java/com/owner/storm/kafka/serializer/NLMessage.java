package com.owner.storm.kafka.serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by napo on 2017/4/28.
 */
public class NLMessage {
    private Map header = new HashMap();
    private byte[] body;

    public Map getHeader() {
        return header;
    }

    public void setHeader(Map header) {
        this.header = header;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
