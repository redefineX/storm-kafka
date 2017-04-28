package com.owner.storm.kafka.serializer;

/**
 * Created by admin on 2017/4/28.
 */
public enum SerialType
{
	string(StringSerializer.class),
	nlmsg(NLMessageSerializer.class);

	private Class clazz;

	public static SerialType getType(String _type)
	{
		for (SerialType serialType : SerialType.values())
		{
			if (_type.equalsIgnoreCase(serialType.name()))
			{
				return serialType;
			}
		}
		return null;
	}

	SerialType(Class clazz)
	{
		this.clazz = clazz;
	}

	public Class getClazz()
	{
		return clazz;
	}

	public void setClazz(Class clazz)
	{
		this.clazz = clazz;
	}
}
