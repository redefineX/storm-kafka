package com.owner.storm.kafka.serializer;

import com.owner.storm.kafka.serializer.*;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/4/28.
 */
public class NLMessageSerializerTest
{
	@Test
	public void serialTest()
	{
		Serializer serializer = new NLMessageSerializer();
		NLMessage msg = new NLMessage();
		Map<String, String> header = new HashMap<>();
		header.put("data_id", "8");
		msg.setHeaders(header);
		msg.setBody(String.format("5,%s,%s,%s,20170414110159,2,11", "das", "das", "dsa").getBytes());

		byte[] bytes = serializer.serialize(msg);

		System.out.println(ToStringBuilder.reflectionToString(serializer.unserialize(bytes)));

	}
}
