package com.owner.storm.tool;

/**
 * Created by admin on 2017/4/26.
 */
public class StormUtils
{
	public static Integer parseInt(Object o)
	{
		if (o == null)
		{
			return null;
		}

		if (o instanceof String)
		{
			return Integer.parseInt(String.valueOf(o));
		}
		else if (o instanceof Long)
		{
			long value = (Long) o;
			return (int) value;
		}
		else if (o instanceof Integer)
		{
			return (Integer) o;
		}
		else
		{
			throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
		}
	}

	public static Integer parseInt(Object o, int defaultValue)
	{
		if (o == null)
		{
			return defaultValue;
		}

		if (o instanceof String)
		{
			return Integer.parseInt(String.valueOf(o));
		}
		else if (o instanceof Long)
		{
			long value = (Long) o;
			return (int) value;
		}
		else if (o instanceof Integer)
		{
			return (Integer) o;
		}
		else
		{
			return defaultValue;
		}
	}

	public static Boolean parseBoolean(Object o)
	{
		if (o == null)
		{
			return null;
		}

		if (o instanceof String)
		{
			return Boolean.valueOf((String) o);
		}
		else if (o instanceof Boolean)
		{
			return (Boolean) o;
		}
		else
		{
			throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
		}
	}

	public static boolean parseBoolean(Object o, boolean defaultValue)
	{
		if (o == null)
		{
			return defaultValue;
		}

		if (o instanceof String)
		{
			return Boolean.valueOf((String) o);
		}
		else if (o instanceof Boolean)
		{
			return (Boolean) o;
		}
		else
		{
			return defaultValue;
		}
	}
}
