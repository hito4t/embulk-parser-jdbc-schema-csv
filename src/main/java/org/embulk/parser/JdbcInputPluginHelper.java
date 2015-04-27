package org.embulk.parser;

import java.lang.reflect.Method;

import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;

public class JdbcInputPluginHelper
{
    private final InputPlugin plugin;
	private final Class<?> abstractJdbcInputPluginClass;

	public JdbcInputPluginHelper(InputPlugin plugin) throws ClassNotFoundException
	{
		this.plugin = plugin;
		abstractJdbcInputPluginClass = plugin.getClass().getClassLoader().loadClass("org.embulk.input.jdbc.AbstractJdbcInputPlugin");
	}

	public Schema getSchema(ConfigSource config) throws Exception
	{
		Class<?> taskClass = getTaskClass();
		Object task = config.loadConfig(taskClass);

		try (AutoCloseable connection = newConnection(task)) {
			return setupTask(connection, task);
		}
	}

	private Class<?> getTaskClass() throws Exception
	{
		Method method = abstractJdbcInputPluginClass.getDeclaredMethod("getTaskClass");
		method.setAccessible(true);
		return (Class<?>)method.invoke(plugin);
	}

	private AutoCloseable newConnection(Object task) throws Exception
	{
		Class<?> taskClass =  plugin.getClass().getClassLoader().loadClass("org.embulk.input.jdbc.AbstractJdbcInputPlugin$PluginTask");
		Method method = abstractJdbcInputPluginClass.getDeclaredMethod("newConnection", taskClass);
		method.setAccessible(true);
		return (AutoCloseable)method.invoke(plugin, task);
	}

	public Schema setupTask(Object connection, Object task) throws Exception
	{
		Class<?> connectionClass =  plugin.getClass().getClassLoader().loadClass("org.embulk.input.jdbc.JdbcInputConnection");
		Class<?> taskClass =  plugin.getClass().getClassLoader().loadClass("org.embulk.input.jdbc.AbstractJdbcInputPlugin$PluginTask");
		Method method = abstractJdbcInputPluginClass.getDeclaredMethod("setupTask", connectionClass, taskClass);
		method.setAccessible(true);
		return (Schema)method.invoke(plugin, connection, task);
	}

}
