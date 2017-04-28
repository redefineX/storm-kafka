package com.owner.storm.tool;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by admin on 2017/3/27.
 */
public class InitParamUtil
{
	private static final Logger logger = LoggerFactory.getLogger (InitParamUtil.class);
	private static ResourceBundle bundle;
	private static BufferedInputStream inputStream;
	private static String etlNum;

	static
	{
		try
		{
			inputStream = new BufferedInputStream (new FileInputStream (""));
			bundle = new PropertyResourceBundle (inputStream);
			logger.debug ("设置配置文件成功.....");
		}
		catch (Exception ex)
		{
			logger.error ("设置配置文件异常:", ex);
		}
		finally
		{
			IOUtils.closeQuietly (inputStream);
		}
	}
}
