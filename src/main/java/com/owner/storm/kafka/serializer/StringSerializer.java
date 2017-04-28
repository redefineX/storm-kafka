package com.owner.storm.kafka.serializer;

import com.nl.util.kafka.NLMessage;

import java.nio.charset.Charset;

/**
 * Created by admin on 2017/4/28.
 */
public class StringSerializer implements Serializer<String>
{
	private final Charset default_c = Charset.forName("utf-8");

	@Override
	public byte[] serialize(String o)
	{
		return o.getBytes(default_c);
	}

	@Override
	public String unserialize(byte[] bytes)
	{
		return new String(bytes, default_c);
	}

}
