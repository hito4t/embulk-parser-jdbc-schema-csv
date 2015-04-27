package org.embulk.parser;

import java.lang.reflect.Method;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class JdbcOutputPluginHelper
{
    private final Logger logger = Exec.getLogger(JdbcOutputPluginHelper.class);

    private final OutputPlugin plugin;
	private final Class<?> abstractJdbcOutputPluginClass;

	public JdbcOutputPluginHelper(OutputPlugin plugin) throws ClassNotFoundException
	{
		this.plugin = plugin;
		abstractJdbcOutputPluginClass = plugin.getClass().getClassLoader().loadClass("org.embulk.output.jdbc.AbstractJdbcOutputPlugin");
	}

	public Schema getSchema(ConfigSource config) throws Exception
	{
		Class<?> taskClass = getTaskClass();
		Object task = config.loadConfig(taskClass);

		Object connector = getConnector(task);
		try (AutoCloseable connection = connec(connector)) {
			Object jdbcSchema = newJdbcSchemaFromExistentTable(connection, config.get(String.class, "table"));
			Object columnSetterFactory = newColumnSetterFactory();

			ImmutableList.Builder<Column> columns = ImmutableList.builder();

			int index = 0;
			for (Object jdbcColumn : getColumns(jdbcSchema)) {
				Type type = getType(columnSetterFactory, jdbcColumn);
				if (type != null) {
					String name = getName(jdbcColumn);
					logger.info(String.format("column %d : name = %s, type = %s", index, name, type));
					columns.add(new Column(index++, name, type));
				}
			}

			return new Schema(columns.build());
		}
	}

	private AutoCloseable connec(Object connector) throws Exception
	{
		Method method = connector.getClass().getMethod("connect", Boolean.TYPE);
		return (AutoCloseable)method.invoke(connector, true);
	}

	private Class<?> getTaskClass() throws Exception
	{
		Method method = abstractJdbcOutputPluginClass.getDeclaredMethod("getTaskClass");
		method.setAccessible(true);
		return (Class<?>)method.invoke(plugin);
	}

	private Object getConnector(Object task) throws Exception
	{
		Class<?> taskClass =  plugin.getClass().getClassLoader().loadClass("org.embulk.output.jdbc.AbstractJdbcOutputPlugin$PluginTask");
		Method method = abstractJdbcOutputPluginClass.getDeclaredMethod("getConnector", taskClass, Boolean.TYPE);
		method.setAccessible(true);
		return method.invoke(plugin, task, false);
	}

	private Object newJdbcSchemaFromExistentTable(Object connection, String tableName) throws Exception
	{
		Class<?> connectionClass =  plugin.getClass().getClassLoader().loadClass("org.embulk.output.jdbc.JdbcOutputConnection");
		Method method = abstractJdbcOutputPluginClass.getDeclaredMethod("newJdbcSchemaFromExistentTable", connectionClass, String.class);
		return method.invoke(plugin, connection, tableName);

	}

	private Object newColumnSetterFactory() throws Exception
	{
		for (Method method : abstractJdbcOutputPluginClass.getDeclaredMethods()) {
			if (method.getName().equals("newColumnSetterFactory")) {
				method.setAccessible(true);
				return method.invoke(plugin, null, null, null);
			}
		}
		throw new RuntimeException();
	}


	private List<Object> getColumns(Object jdbcSchema) throws Exception
	{
		Method method = jdbcSchema.getClass().getMethod("getColumns");
		return (List<Object>)method.invoke(jdbcSchema);
	}

	private String getName(Object jdbcColumn) throws Exception
	{
		Method method = jdbcColumn.getClass().getMethod("getName");
		return (String)method.invoke(jdbcColumn);
	}

	private Type getType(Object columnSetterFactory, Object jdbcColumn) throws Exception
	{
		Method method = columnSetterFactory.getClass().getMethod("newColumnSetter", jdbcColumn.getClass());
		Object columnSetter = method.invoke(columnSetterFactory, jdbcColumn);
		String className = columnSetter.getClass().getSimpleName();
		if (className.startsWith("Boolean")) {
			return Types.BOOLEAN;
		}
		if (className.startsWith("String")) {
			return Types.STRING;
		}
		if (className.startsWith("Long")) {
			return Types.LONG;
		}
		if (className.startsWith("Double")) {
			return Types.DOUBLE;
		}
		if (className.startsWith("SqlTimestamp")) {
			return Types.TIMESTAMP;
		}

		return null;
	}

}
