Embulk::JavaPlugin.register_parser(
  "jdbc-schema-csv", "org.embulk.parser.JdbcSchemaCsvParser",
  File.expand_path('../../../../classpath', __FILE__))
