package com.owner.storm.kafka.serializer;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/4/28.
 */
public class NLMessageSerializer implements Serializer<NLMessage>
{
	private Schema<NLMessage> schema = null;

	public NLMessageSerializer()
	{
		schema = RuntimeSchema.getSchema(NLMessage.class);
	}

	@Override
	public byte[] serialize(NLMessage nlMessage)
	{
		LinkedBuffer buffer = LinkedBuffer.allocate(2048 + nlMessage.getBody().length);
		return ProtobufIOUtil.toByteArray(nlMessage, schema, buffer);
	}

	@Override
	public NLMessage unserialize(byte[] bytes)
	{
		NLMessage nlMessage = new NLMessage();
		ProtobufIOUtil.mergeFrom(bytes, nlMessage, schema);
		return nlMessage;
	}

}
