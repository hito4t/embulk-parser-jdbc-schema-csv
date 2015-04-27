package org.embulk.parser;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;

public class JdbcSchemaCsvParser extends CsvParserPlugin
{
	@Override
	public void transaction(ConfigSource config, final Control control)
	{
		ExecSession session = Exec.session();
		ConfigSource child = config.getNested("schema");
		String type = child.get(String.class, "type");
		OutputPlugin output = session.newPlugin(OutputPlugin.class, new PluginType(type));

		try {
			JdbcOutputPluginHelper helper = new JdbcOutputPluginHelper(output);
			final Schema schemaFromJdbc = helper.getSchema(child);
			super.transaction(config, new Control() {
				@Override
				public void run(TaskSource taskSource, Schema schema) {
					control.run(taskSource, schemaFromJdbc);
				}
			});

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
