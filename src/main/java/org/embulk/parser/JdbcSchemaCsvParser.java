package org.embulk.parser;


import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;
import org.slf4j.Logger;

public class JdbcSchemaCsvParser extends CsvParserPlugin
{
    private final Logger logger = Exec.getLogger(JdbcSchemaCsvParser.class);

	@Override
	public void transaction(ConfigSource config, final Control control)
	{
		ExecSession session = Exec.session();
		ConfigSource child = config.getNested("schema");
		String type = child.get(String.class, "type");
		InputPlugin input = session.newPlugin(InputPlugin.class, new PluginType(type));

		try {
			JdbcInputPluginHelper helper = new JdbcInputPluginHelper(input);
			final Schema schemaFromJdbc = helper.getSchema(child);
			for (int i = 0; i < schemaFromJdbc.getColumnCount(); i++) {
				Column column = schemaFromJdbc.getColumn(i);
				logger.info(String.format("column %d : name = %s, type = %s", i + 1, column.getName(), column.getType()));
			}

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
