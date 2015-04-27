package org.embulk.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkPluginTester;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class JdbcSchemaCsvParserTest
{

/*
Prepare MySQL table:

create table embulk_test;
grant all on embulk_test.* to embulk_user@"%" identified by 'embulk_pass';

create table embulk_test.input_test (
    id        int,
    name      char(4),
    value     decimal(8, 2),
    creation  timestamp
);
*/

	private static EmbulkPluginTester tester = new EmbulkPluginTester(ParserPlugin.class, "d", JdbcSchemaCsvParser.class);

	@Test
	public void testCsv() throws Exception
	{
		test("/yml/csv.yml");
	}

	private void test(String yml) throws Exception
	{
		tester.run(yml);

		FileSystem fs = FileSystems.getDefault();
		List<String> lines = Files.readAllLines(fs.getPath("result.000.00.tsv"), Charset.forName("UTF-8"));
		assertEquals(2, lines.size());
		assertEquals("1\ttest\t123.4\t2015-04-27 11:23:45", lines.get(0));
		assertEquals("2\tsample\t-1.0\t2015-12-31 23:59:59", lines.get(1));
	}
}
