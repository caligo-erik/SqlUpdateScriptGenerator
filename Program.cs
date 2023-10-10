using CommandLine;
using Dapper;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Text;

namespace Caligo.SqlUpdateScriptGenerator
{
  enum ExitCode : int
  {
    Success = 0,
    MissingArguments = 1,
    SqlConnectionError = 2,
    IOError = 4
  }

  class Options
  {
    [Option('s', "source", Required = true, HelpText = "Database source connection string.")]
    public string SourceConnectionString { get; set; }

    [Option('d', "destination", Required = true, HelpText = "Database destination connection string.")]
    public string DestinationConnectionString { get; set; }

    [Option('o', "output", Default = "update.sql", HelpText = "Output file path.")]
    public string OutputPath { get; set; }
  }

  class Program
  {
    static void Main(string[] args)
    {
      Parser.Default.ParseArguments<Options>(args)
      .WithParsed(options =>
      {
        string sourceConnectionString = options.SourceConnectionString;
        string destinationConnectionString = options.DestinationConnectionString;
        string outputPath = options.OutputPath;

        if (string.IsNullOrWhiteSpace(sourceConnectionString) || string.IsNullOrWhiteSpace(destinationConnectionString))
        {
          Environment.Exit((int)ExitCode.MissingArguments);
          return;
        }
        GenerateUpdateScript(sourceConnectionString, destinationConnectionString, outputPath);
      });
    }

    private static void GenerateUpdateScript(string sourceConnectionString, string destinationConnectionString, string outputPath)
    {
      try
      {
        var sourceTables = GetTables(sourceConnectionString);
        var destinationTables = GetTables(destinationConnectionString);

        var script = GenerateSchemaUpdateScript(sourceTables, destinationTables);
        Console.WriteLine(script);
      }
      catch (Exception ex)
      {
        Console.WriteLine($"An error occurred: {ex.Message}");
        Console.WriteLine(ex.StackTrace);
        Environment.Exit((int)ExitCode.SqlConnectionError);
      }
    }

    private static Dictionary<string, Table> GetTables(string connectionString, string schemaName = null)
    {
      Dictionary<string, Table> tables = new Dictionary<string, Table>();
      using SqlConnection connection = new SqlConnection(connectionString);
      connection.Open();
      var schemaCheck = !string.IsNullOrWhiteSpace(schemaName) ? $"and t.TABLE_SCHEMA = '{schemaName}' " : string.Empty;

      var FKQuery = $@"SELECT  obj.name AS FK_NAME,
    sch.name AS [SCHEMA_NAME],
    tab1.name AS [TABLE_NAME],
    col1.name AS [COLUMN_NAME],
    tab2.name AS [REFERENCED_TABLE_NAME],
    col2.name AS [REFERENCED_COLUMN_NAME]
FROM sys.foreign_key_columns fkc
INNER JOIN sys.objects obj
    ON obj.object_id = fkc.constraint_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fkc.parent_object_id
INNER JOIN sys.schemas sch
    ON tab1.schema_id = sch.schema_id
INNER JOIN sys.columns col1
    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fkc.referenced_object_id
INNER JOIN sys.columns col2
    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
WHERE tab1.type_desc = 'USER_TABLE' " + schemaCheck;

      var columns = connection.Query<TableColumn>(@"SELECT t.TABLE_NAME, t.TABLE_SCHEMA, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE FROM INFORMATION_SCHEMA.TABLES t 
	inner join INFORMATION_SCHEMA.COLUMNS c on t.TABLE_NAME = c.TABLE_NAME
	WHERE t.TABLE_TYPE = 'BASE TABLE' " + schemaCheck + @"
	order by t.TABLE_NAME, c.COLUMN_NAME");

      foreach (var column in columns)
      {
        var tableName = column.TABLE_NAME;

        if (!tables.ContainsKey(tableName))
        {
          tables.Add(tableName, new Table
          {
            Name = tableName,
            Schema = column.TABLE_SCHEMA,
          });
        }
        var table = tables[tableName];
        table.Columns.Add(column.COLUMN_NAME, column);
      }

      var primaryKeys = connection.Query<dynamic>(@$"SELECT 
                k.TABLE_NAME, k.COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k inner join INFORMATION_SCHEMA.TABLES t on k.TABLE_NAME = t.TABLE_NAME
                WHERE t.TABLE_TYPE = 'BASE TABLE' " + schemaCheck + @" AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1;");

      foreach (var key in primaryKeys)
      {
        var table = tables[key.TABLE_NAME];
        table.PrimaryKeys.Add(key.COLUMN_NAME);
      }

      var foreignKeys = connection.Query<ForeignKey>(FKQuery);
      foreach (var fk in foreignKeys)
      {
        var table = tables[fk.TABLE_NAME];
        if (table.ForeignKeys.ContainsKey(fk.COLUMN_NAME)) {
          continue;
        }
        table.ForeignKeys.Add(fk.COLUMN_NAME, fk);
      }

      return tables;
    }
    private static string GenerateSchemaUpdateScript(Dictionary<string, Table> sourceTables, Dictionary<string, Table> destinationTables)
    {
      StringBuilder scriptBuilder = new StringBuilder();

      // Loop through destination tables to identify tables to be added or modified
      foreach (var destinationTableEntry in destinationTables)
      {
        string tableName = destinationTableEntry.Key;
        Table destinationTable = destinationTableEntry.Value;

        // Check if the table exists in the source schema
        if (sourceTables.TryGetValue(tableName, out Table sourceTable))
        {
          // Pass the StringBuilder to generate updates
          GenerateColumnUpdates(sourceTable, destinationTable, scriptBuilder);
          GeneratePrimaryKeyUpdates(sourceTable, destinationTable, scriptBuilder);
          GenerateForeignKeyUpdates(sourceTable, destinationTable, scriptBuilder);
        }
        else
        {
          // The table exists in the destination but not in the source, generate SQL to create it if needed
          // Append the script parts to the overall update script
          GenerateTableCreation(destinationTable, scriptBuilder);
        }
      }

      // Loop through source tables to identify tables to be deleted
      foreach (var sourceTableEntry in sourceTables)
      {
        string tableName = sourceTableEntry.Key;
        Table sourceTable = sourceTableEntry.Value;

        // Check if the table exists in the destination schema
        if (!destinationTables.ContainsKey(tableName))
        {
          // The table exists in the source but not in the destination, generate SQL to delete it if needed
          // Append the script parts to the overall update script
          GenerateTableDeletion(sourceTable, scriptBuilder);
        }
      }

      return scriptBuilder.ToString();
    }

    private static void GenerateColumnUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      // Get the column names from source and destination tables
      HashSet<string> sourceColumnNames = new HashSet<string>(sourceTable.Columns.Keys);
      HashSet<string> destinationColumnNames = new HashSet<string>(destinationTable.Columns.Keys);

      // Find columns that need to be added, modified, or deleted
      HashSet<string> columnsToAdd = new HashSet<string>(destinationColumnNames.Except(sourceColumnNames));
      HashSet<string> columnsToDelete = new HashSet<string>(sourceColumnNames.Except(destinationColumnNames));
      HashSet<string> columnsToModify = new HashSet<string>(sourceColumnNames.Intersect(destinationColumnNames));

      // Generate SQL for adding columns
      foreach (string columnToAdd in columnsToAdd)
      {
        TableColumn column = destinationTable.Columns[columnToAdd];
        GenerateAddColumnScript(destinationTable.Name, column, scriptBuilder);
      }

      // Generate SQL for modifying columns
      foreach (string columnToModify in columnsToModify)
      {
        TableColumn sourceColumn = sourceTable.Columns[columnToModify];
        TableColumn destinationColumn = destinationTable.Columns[columnToModify];

        // Compare sourceColumn and destinationColumn properties to determine if modification is needed
        // if (/* Check if modification is needed based on properties */)
        // {
        //   GenerateModifyColumnScript(destinationTable.Name, sourceColumn, destinationColumn, scriptBuilder);
        // }
      }

      // Generate SQL for deleting columns
      foreach (string columnToDelete in columnsToDelete)
      {
        GenerateDeleteColumnScript(destinationTable.Name, columnToDelete, scriptBuilder);
      }
    }

    private static void GenerateAddColumnScript(string tableName, TableColumn column, StringBuilder scriptBuilder)
    {
      scriptBuilder.AppendLine($"-- ADD COLUMN {column.COLUMN_NAME} TO {tableName}");
      string columnName = column.COLUMN_NAME;
      string dataType = column.DATA_TYPE;
      int? characterMaxLength = column.CHARACTER_MAXIMUM_LENGTH;
      int? numericPrecision = column.NUMERIC_PRECISION;
      int? numericScale = column.NUMERIC_SCALE;
      bool isNullable = column.IS_NULLABLE?.ToLower() == "yes";

      // Start building the ADD COLUMN script
      scriptBuilder.Append($"ALTER TABLE {tableName} ADD {columnName} {dataType}");

      // Handle data type-specific considerations
      switch (dataType.ToLower())
      {
        case "char":
        case "nchar":
        case "varchar":
        case "nvarchar":
          if (characterMaxLength.HasValue)
          {
            scriptBuilder.Append($"({characterMaxLength})");
          }
          break;

        case "decimal":
        case "float":
        case "money":
        case "numeric":
        case "smallmoney":
        case "real":
          if (numericPrecision.HasValue && numericScale.HasValue)
          {
            scriptBuilder.Append($"({numericPrecision}, {numericScale})");
          }
          break;

        case "bigint":
        case "bit":
        case "smallint":
        case "int":
        case "tinyint":
        case "date":
        case "datetime":
        case "datetime2":
        case "time":
        case "uniqueidentifier":
          break;
        default:
          // Handle unsupported or generic data types
          break;
      }

      // Specify NULL/NOT NULL based on the IS_NULLABLE property
      scriptBuilder.Append(isNullable ? " NULL" : " NOT NULL");

      // Complete the script with a semicolon
      scriptBuilder.AppendLine(";");
      scriptBuilder.AppendLine("GO");
    }



    // Generate SQL script for adding a new column to the table
    // Example: ALTER TABLE tableName ADD COLUMN_NAME DATATYPE;
    // Append the SQL script to the StringBuilder

    private static void GenerateModifyColumnScript(string tableName, TableColumn sourceColumn, TableColumn destinationColumn, StringBuilder scriptBuilder)
    {
      // Generate SQL script for modifying a column in the table
      // Example: ALTER TABLE tableName ALTER COLUMN COLUMN_NAME NEW_DATATYPE;
      // Append the SQL script to the StringBuilder
    }

    private static void GenerateDeleteColumnScript(string tableName, string columnName, StringBuilder scriptBuilder)
    {
      // Generate SQL script for deleting a column from the table
      // Example: ALTER TABLE tableName DROP COLUMN COLUMN_NAME;
      // Append the SQL script to the StringBuilder
    }


    private static void GeneratePrimaryKeyUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      // Compare and generate SQL for primary key updates (e.g., ADD, DROP)
      // Append the SQL script parts to the StringBuilder
    }

    private static void GenerateForeignKeyUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      // Compare and generate SQL for foreign key updates (e.g., ADD, DROP)
      // Append the SQL script parts to the StringBuilder
    }

    private static void GenerateTableCreation(Table destinationTable, StringBuilder scriptBuilder)
    {
      // Generate SQL for creating a table
      // Return a string containing the SQL script parts
    }

    private static void GenerateTableDeletion(Table sourceTable, StringBuilder scriptBuilder)
    {
      // Generate SQL for deleting a table
      // Return a string containing the SQL script parts
    }

  }

  internal class TableColumn
  {
    public string? TABLE_NAME, TABLE_SCHEMA, COLUMN_NAME, DATA_TYPE, IS_NULLABLE;
    public int? CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE;
  }

  internal class Table
  {
    public string? Name, Schema;
    public Dictionary<string, TableColumn> Columns = new Dictionary<string, TableColumn>();

    public HashSet<string> PrimaryKeys = new HashSet<string>();
    public Dictionary<string, ForeignKey> ForeignKeys = new Dictionary<string, ForeignKey>();
  }

  internal class ForeignKey
  {
    public string FK_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME;
  }
}