package com.owner.storm.kafka.serializer;

import com.nl.util.kafka.NLMessage;

/**
 * Created by admin on 2017/4/28.
 */
public interface Serializer<T>
{
	/**
	 * 序列化
	 *
	 * @param t
	 * @return
	 */
	public byte[] serialize(T t);

	/**
	 * 反序列化
	 *
	 * @param bytes
	 * @return
	 */
	public T unserialize(byte[] bytes);

}
