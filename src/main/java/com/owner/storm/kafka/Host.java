package com.owner.storm.kafka;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Created by admin on 2017/3/27.
 */
public class Host
{
	private String host;
	private int port;

	public Host ( String host, int port )
	{
		this.host = host;
		this.port = port;
	}

	public String getHost ()
	{
		return host;
	}

	public void setHost ( String host )
	{
		this.host = host;
	}

	public int getPort ()
	{
		return port;
	}

	public void setPort ( int port )
	{
		this.port = port;
	}

	@Override
	public String toString()
	{
		return ToStringBuilder.reflectionToString(this);
	}
}
