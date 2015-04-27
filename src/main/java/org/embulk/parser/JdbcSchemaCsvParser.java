package org.embulk.parser;

import org.embulk.config.ConfigSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.standards.CsvParserPlugin;

public class JdbcSchemaCsvParser extends CsvParserPlugin {

	@Override
	public void transaction(ConfigSource config, Control control) {
		ExecSession session = Exec.session();
		String type = config.get(String.class, "type");
		ConfigSource child = config.getNested("schema");
		String type2 = child.get(String.class, "type");
		OutputPlugin o = session.newPlugin(OutputPlugin.class, new PluginType(type2));

		System.out.println(o);
		/*
		//config.
		//o.transaction(config, null, 0, null);

		try {
			new Helper().getSchema(config);
		} catch (SQLException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		*/


		//control.
		// TODO 自動生成されたメソッド・スタブ
		// TODO 自動生成されたメソッド・スタブ
		super.transaction(config, control);
	}

}
