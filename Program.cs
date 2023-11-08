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
        var script = GenerateUpdateScript(sourceConnectionString, destinationConnectionString, outputPath);
        if (!string.IsNullOrWhiteSpace(script))
        {
          string filePath = !string.IsNullOrEmpty(outputPath) ? outputPath : "update.sql";

          Console.WriteLine($"Writing to file: {filePath}");
          // Write the YAML string to the file
          File.WriteAllText(filePath, script);
        }
      });
    }

    private static string GenerateUpdateScript(string sourceConnectionString, string destinationConnectionString, string outputPath)
    {
      try
      {
        var sourceTables = GetTables(sourceConnectionString);
        var destinationTables = GetTables(destinationConnectionString);

        return GenerateSchemaUpdateScript(sourceTables, destinationTables);
      }
      catch (Exception ex)
      {
        Console.WriteLine($"An error occurred: {ex.Message}");
        Console.WriteLine(ex.StackTrace);
        Environment.Exit((int)ExitCode.SqlConnectionError);
        return null;
      }
    }

    private static Dictionary<string, Table> GetTables(string connectionString, string schemaName = null)
    {
      Dictionary<string, Table> tables = new Dictionary<string, Table>();
      using SqlConnection connection = new SqlConnection(connectionString);
      connection.Open();
      var schemaCheck = !string.IsNullOrWhiteSpace(schemaName) ? $"and t.TABLE_SCHEMA = '{schemaName}' " : string.Empty;

      var FKQuery = $@"SELECT  
    obj.name AS FK_NAME,
    sch.name AS [SCHEMA_NAME],
    tab1.name AS [TABLE_NAME],
    col1.name AS [COLUMN_NAME],
    tab2.name AS [REFERENCED_TABLE_NAME],
    col2.name AS [REFERENCED_COLUMN_NAME],
    rc.UPDATE_RULE AS UPDATE_CASCADE_ACTION,
    rc.DELETE_RULE AS DELETE_CASCADE_ACTION
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
INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    ON rc.CONSTRAINT_NAME = obj.name
WHERE tab1.type_desc = 'USER_TABLE'" + schemaCheck;

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
        table.ForeignKeys.Add(fk.FK_NAME, fk);
      }

      schemaCheck = !string.IsNullOrWhiteSpace(schemaName) ? $"AND SCHEMA_NAME(t.schema_id) = '{schemaName}' " : string.Empty;

      var columnIndices = connection.Query<ColumnIndex>(@$"SELECT
    SCHEMA_NAME(t.schema_id) AS TABLE_SCHEMA,
    OBJECT_NAME(ix.object_id) AS TABLE_NAME,
    c.name AS COLUMN_NAME,
	ix.name AS INDEX_NAME,
    CASE WHEN ix.is_unique = 1 THEN 1 ELSE 0 END AS IS_UNIQUE,
    CASE WHEN ix.type_desc = 'NONCLUSTERED' THEN 1 ELSE 0 END AS IS_NONCLUSTERED
FROM sys.indexes AS ix
INNER JOIN sys.index_columns AS ic
    ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
INNER JOIN sys.columns AS c
    ON ix.object_id = c.object_id AND ic.column_id = c.column_id
INNER JOIN sys.tables AS t
    ON ix.object_id = t.object_id
WHERE ix.is_primary_key = 0 -- Exclude primary keys
    AND (ix.is_unique = 1 OR ix.type_desc = 'NONCLUSTERED') -- Include unique or non-clustered indexes
    AND t.is_ms_shipped = 0 -- Exclude system tables
ORDER by TABLE_NAME, COLUMN_NAME
" + schemaCheck);

      foreach (var index in columnIndices)
      {
        var table = tables[index.TABLE_NAME];
        try
        {
          table.Indices.Add(index);
        }
        catch (Exception ex)
        {
          Console.Write("");
        }
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
          GenerateIndexUpdates(sourceTable, destinationTable, scriptBuilder);
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

      if (columnsToAdd.Count > 0)
      {
        scriptBuilder.AppendLine($"-- Add columns to {destinationTable.Name}");
        scriptBuilder.AppendLine($"ALTER TABLE [{destinationTable.Name}]");
        var columnsToAddList = columnsToAdd.ToList();
        for (int i = 0; i < columnsToAddList.Count; i++)
        {
          // Generate SQL for adding columns
          string columnToAdd = columnsToAddList[i];
          TableColumn column = destinationTable.Columns[columnToAdd];
          GenerateAddColumnScript(column, scriptBuilder, columnsToAddList.IsLast(i));
        }
        scriptBuilder.AppendLine();
      }

      // Generate SQL for modifying columns
      foreach (string columnToModify in columnsToModify)
      {
        TableColumn sourceColumn = sourceTable.Columns[columnToModify];
        TableColumn destinationColumn = destinationTable.Columns[columnToModify];

        if (ColumnsMatch(sourceColumn, destinationColumn))
        {
          // scriptBuilder.AppendLine($"-- Skipping {destinationTable.Name} {sourceColumn.COLUMN_NAME} since the column hasn't changed");
          continue;
        }

        if (sourceColumn.DATA_TYPE != destinationColumn.DATA_TYPE)
        {
          GenerateModifyColumnDataTypeScript(destinationTable.Name, sourceColumn, destinationColumn, scriptBuilder);
        }
        else
        {
          GenerateModifyColumnScript(destinationTable.Name, sourceColumn, destinationColumn, scriptBuilder);
        }

        // Compare sourceColumn and destinationColumn properties to determine if modification is needed
        // if (/* Check if modification is needed based on properties */)
        // {
        //   GenerateModifyColumnScript(destinationTable.Name, sourceColumn, destinationColumn, scriptBuilder);
        // }
      }

      // Generate SQL for deleting columns
      if (columnsToDelete.Count > 0)
      {
        var columnsToDeleteList = columnsToDelete.ToList();
        scriptBuilder.AppendLine();
        scriptBuilder.AppendLine($"-- ! DROP COLUMNS FROM {destinationTable.Name}");
        scriptBuilder.AppendLine($"ALTER TABLE [{destinationTable.Name}]");
        for (int i = 0; i < columnsToDeleteList.Count; i++)
        {
          string columnToDelete = columnsToDeleteList[i];
          scriptBuilder.Append($" DROP COLUMN [{columnToDelete}]");
          scriptBuilder.AppendLine(columnsToDeleteList.IsLast(i) ? ";" : ",");
        }
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
    }

    private static void GenerateModifyColumnDataTypeScript(string? tableName, TableColumn sourceColumn, TableColumn destinationColumn, StringBuilder scriptBuilder)
    {
      scriptBuilder.AppendLine($"-- Update table {tableName} alter data type for column {sourceColumn.COLUMN_NAME}");
      scriptBuilder.AppendLine("-- This will most likely fail, check each statement");

      // create temp column to copy old data
      string tempColumnName = $"{sourceColumn.COLUMN_NAME}_{System.Guid.NewGuid()}";
      scriptBuilder.AppendLine($"ALTER TABLE [{tableName}] ADD COLUMN [{tempColumnName}] {sourceColumn.DATA_TYPE} NULL");
      scriptBuilder.AppendLine($"UPDATE [{tableName}] SET [{tempColumnName}] = [{sourceColumn.COLUMN_NAME}];");
      scriptBuilder.AppendLine($"ALTER TABLE [{tableName}] DROP COLUMN [{sourceColumn.COLUMN_NAME}];");
      scriptBuilder.AppendLine($"ALTER TABLE [{tableName}] ADD COLUMN [{sourceColumn.COLUMN_NAME}] {destinationColumn.DATA_TYPE} NULL");

      // special case from bool to datetime
      if (sourceColumn.DATA_TYPE == "bit" && destinationColumn.DATA_TYPE == "datetime")
      {
        scriptBuilder.AppendLine($"UPDATE [{tableName}] SET [{sourceColumn.COLUMN_NAME}] = CASE WHEN [{tempColumnName}] = 1 THEN GETDATE() ELSE NULL END;");
      }
      else
      {
        scriptBuilder.AppendLine($"UPDATE [{tableName}] SET [{sourceColumn.COLUMN_NAME}] = CAST([{tempColumnName}] AS {destinationColumn.DATA_TYPE});");
      }
      scriptBuilder.AppendLine();
    }

    private static bool ColumnsMatch(TableColumn sourceColumn, TableColumn destinationColumn)
    {
      return
        sourceColumn.CHARACTER_MAXIMUM_LENGTH == destinationColumn.CHARACTER_MAXIMUM_LENGTH &&
        sourceColumn.DATA_TYPE == destinationColumn.DATA_TYPE &&
        sourceColumn.IS_NULLABLE == destinationColumn.IS_NULLABLE &&
        sourceColumn.NUMERIC_PRECISION == destinationColumn.NUMERIC_PRECISION &&
        sourceColumn.NUMERIC_SCALE == destinationColumn.NUMERIC_SCALE;
    }

    private static void GenerateAddColumnScript(TableColumn column, StringBuilder scriptBuilder, bool isLast)
    {
      string columnName = column.COLUMN_NAME;
      string dataType = column.DATA_TYPE;
      int? characterMaxLength = column.CHARACTER_MAXIMUM_LENGTH;
      int? numericPrecision = column.NUMERIC_PRECISION;
      int? numericScale = column.NUMERIC_SCALE;
      bool isNullable = column.IS_NULLABLE?.ToLower() == "yes";

      // Start building the ADD COLUMN script
      scriptBuilder.Append($" ADD [{columnName}] {dataType}");

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
      scriptBuilder.AppendLine(isLast ? ";" : ",");
    }



    // Generate SQL script for adding a new column to the table
    // Example: ALTER TABLE tableName ADD COLUMN_NAME DATATYPE;
    // Append the SQL script to the StringBuilder

    private static void GenerateModifyColumnScript(string tableName, TableColumn sourceColumn, TableColumn destinationColumn, StringBuilder scriptBuilder)
    {
      if (sourceColumn.NUMERIC_PRECISION != destinationColumn.NUMERIC_PRECISION || sourceColumn.NUMERIC_SCALE != destinationColumn.NUMERIC_SCALE)
      {
        scriptBuilder.Append($"ALTER TABLE [{tableName}] ALTER COLUMN [{sourceColumn.COLUMN_NAME}] NUMERIC({destinationColumn.NUMERIC_PRECISION}, {destinationColumn.NUMERIC_SCALE})");
        scriptBuilder.AppendLine(";");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
      if (sourceColumn.CHARACTER_MAXIMUM_LENGTH != destinationColumn.CHARACTER_MAXIMUM_LENGTH)
      {
        scriptBuilder.Append($"ALTER TABLE [{tableName}] ALTER COLUMN [{sourceColumn.COLUMN_NAME}] {destinationColumn.DATA_TYPE}({destinationColumn.CHARACTER_MAXIMUM_LENGTH})");
        scriptBuilder.AppendLine(";");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
      if (sourceColumn.IS_NULLABLE != destinationColumn.IS_NULLABLE)
      {
        bool isNullable = destinationColumn.IS_NULLABLE.ToLower() == "yes";
        scriptBuilder.Append($"ALTER TABLE [{tableName}] ALTER COLUMN [{sourceColumn.COLUMN_NAME}] ");
        scriptBuilder.Append(isNullable ? " NULL" : " NOT NULL");
        scriptBuilder.AppendLine(";");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
    }


    private static void GeneratePrimaryKeyUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      HashSet<string> idsToAdd = new HashSet<string>(destinationTable.PrimaryKeys.Except(sourceTable.PrimaryKeys));
      HashSet<string> idsToDelete = new HashSet<string>(sourceTable.PrimaryKeys.Except(destinationTable.PrimaryKeys));

      if (idsToAdd.Count > 0 || idsToDelete.Count > 0)
      {
        scriptBuilder.AppendLine($"-- updating primary keys for table {destinationTable.Name}");
        scriptBuilder.AppendLine($"ALTER TABLE {destinationTable.Name} DROP CONSTRAINT PK_{destinationTable.Name}");
        scriptBuilder.AppendLine($"ALTER TABLE {destinationTable.Name} ADD CONSTRAINT PK_{destinationTable.Name} PRIMARY KEY ({string.Join(", ", destinationTable.PrimaryKeys)})");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
    }

    private static void GenerateForeignKeyUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      Dictionary<string, ForeignKey> sourceForeignKeys = new Dictionary<string, ForeignKey>();
      List<ForeignKey> sourceForeignKeyWarnings = new List<ForeignKey>();
      if (sourceTable != null)
      {
        foreach (var sourceForeignKey in sourceTable.ForeignKeys.Values)
        {
          string foreignKeyId = GetForeignKeyId(sourceForeignKey);
          if (sourceForeignKeys.ContainsKey(foreignKeyId))
          {
            sourceForeignKeyWarnings.Add(sourceForeignKey);
            continue;
          }
          sourceForeignKeys.Add(foreignKeyId, sourceForeignKey);
        }
      }

      Dictionary<string, ForeignKey> destinationForeignKeys = new Dictionary<string, ForeignKey>();
      List<ForeignKey> destinationForeignKeyWarnings = new List<ForeignKey>();
      foreach (var destinationForeignKey in destinationTable.ForeignKeys.Values)
      {
        string foreignKeyId = GetForeignKeyId(destinationForeignKey);
        if (destinationForeignKeys.ContainsKey(foreignKeyId))
        {
          destinationForeignKeyWarnings.Add(destinationForeignKey);
          continue;
        }
        destinationForeignKeys.Add(foreignKeyId, destinationForeignKey);
      }
      foreach (var warningForeignKey in sourceForeignKeyWarnings)
      {
        scriptBuilder.AppendLine($"-- WARNING: source table {destinationTable.Name} has multiple foreign keys to {warningForeignKey.REFERENCED_TABLE_NAME}:{warningForeignKey.REFERENCED_COLUMN_NAME}");
      }
      foreach (var warningForeignKey in destinationForeignKeyWarnings)
      {
        scriptBuilder.AppendLine($"-- WARNING: destination table {destinationTable.Name} has multiple foreign keys to {warningForeignKey.REFERENCED_TABLE_NAME}:{warningForeignKey.REFERENCED_COLUMN_NAME}");
      }

      HashSet<string> foreignKeysToAdd = new HashSet<string>(destinationForeignKeys.Keys.Except(sourceForeignKeys.Keys));
      HashSet<string> foreignKeysToDelete = new HashSet<string>(sourceForeignKeys.Keys.Except(destinationForeignKeys.Keys));
      HashSet<string> foreignKeysToModify = new HashSet<string>(sourceForeignKeys.Keys.Intersect(destinationForeignKeys.Keys));

      foreach (var foreignKeyId in foreignKeysToAdd)
      {
        var foreignKey = destinationForeignKeys[foreignKeyId];
        scriptBuilder.AppendLine($"ALTER TABLE [{foreignKey.TABLE_NAME}] ADD CONSTRAINT [{foreignKey.FK_NAME}] FOREIGN KEY ({foreignKey.COLUMN_NAME}) " +
          $"REFERENCES [{foreignKey.REFERENCED_TABLE_NAME}]({foreignKey.REFERENCED_COLUMN_NAME}) " +
          $"ON UPDATE {foreignKey.UPDATE_CASCADE_ACTION} ON DELETE {foreignKey.DELETE_CASCADE_ACTION};");

      }
      foreach (var foreignKeyId in foreignKeysToDelete)
      {
        var foreignKey = sourceForeignKeys[foreignKeyId];
        scriptBuilder.AppendLine($"ALTER TABLE [{foreignKey.TABLE_NAME}] DROP CONSTRAINT [{foreignKey.FK_NAME}]");
      }
      foreach (var foreignKeyId in foreignKeysToModify)
      {
        var sourceForeignKey = sourceForeignKeys[foreignKeyId];
        var destinationForeignKey = destinationForeignKeys[foreignKeyId];
        if (sourceForeignKey.DELETE_CASCADE_ACTION != destinationForeignKey.DELETE_CASCADE_ACTION
         || sourceForeignKey.UPDATE_CASCADE_ACTION != destinationForeignKey.UPDATE_CASCADE_ACTION)
        {
          scriptBuilder.AppendLine($"ALTER TABLE [{destinationForeignKey.TABLE_NAME}] DROP CONSTRAINT [{sourceForeignKey.FK_NAME}]");
          scriptBuilder.AppendLine($"ALTER TABLE [{destinationForeignKey.TABLE_NAME}] ADD CONSTRAINT [{destinationForeignKey.FK_NAME}] FOREIGN KEY ({destinationForeignKey.COLUMN_NAME}) " +
          $"REFERENCES [{destinationForeignKey.REFERENCED_TABLE_NAME}]({destinationForeignKey.REFERENCED_COLUMN_NAME}) " +
          $"ON UPDATE {destinationForeignKey.UPDATE_CASCADE_ACTION} ON DELETE {destinationForeignKey.DELETE_CASCADE_ACTION};");
          scriptBuilder.AppendLine("GO");
          scriptBuilder.AppendLine();
        }
      }
    }

    private static void GenerateIndexUpdates(Table sourceTable, Table destinationTable, StringBuilder scriptBuilder)
    {
      Dictionary<string, ColumnIndex> sourceIndices = new Dictionary<string, ColumnIndex>();
      List<ColumnIndex> sourceIndexWarnings = new List<ColumnIndex>();
      if (sourceTable != null)
      {
        foreach (var sourceIndex in sourceTable.Indices)
        {
          string columnIndexId = GetColumnIndexId(sourceIndex);
          if (sourceIndices.ContainsKey(columnIndexId))
          {
            sourceIndexWarnings.Add(sourceIndex);
            continue;
          }
          else
          {
            sourceIndices.Add(columnIndexId, sourceIndex);
          }
        }
      }
      Dictionary<string, ColumnIndex> destinationIndices = new Dictionary<string, ColumnIndex>();
      List<ColumnIndex> destinationIndexWarnings = new List<ColumnIndex>();
      foreach (var destinationIndex in destinationTable.Indices)
      {
        string columnIndexId = GetColumnIndexId(destinationIndex);
        if (destinationIndices.ContainsKey(columnIndexId))
        {
          destinationIndexWarnings.Add(destinationIndex);
          continue;
        }
        else
        {
          destinationIndices.Add(columnIndexId, destinationIndex);
        }
      }

      HashSet<string> indicesToAdd = new HashSet<string>(destinationIndices.Keys.Except(sourceIndices.Keys));
      HashSet<string> indicesToDelete = new HashSet<string>(sourceIndices.Keys.Except(destinationIndices.Keys));

      foreach (var indexName in indicesToAdd)
      {
        var index = destinationTable.Indices.FirstOrDefault(x => indexName == GetColumnIndexId(x));
        string indexType = index.IS_NONCLUSTERED ? "NONCLUSTERED" : "CLUSTERED";
        string uniqueConstraint = index.IS_UNIQUE ? "UNIQUE" : "";

        scriptBuilder.AppendLine($"CREATE {uniqueConstraint} {indexType} INDEX {index.INDEX_NAME} " +
         $"ON [{index.TABLE_SCHEMA}].[{index.TABLE_NAME}] ({index.COLUMN_NAME});");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
      foreach (var indexName in indicesToDelete)
      {
        var index = sourceTable.Indices.FirstOrDefault(x => indexName == GetColumnIndexId(x));
        scriptBuilder.AppendLine($"DROP INDEX [{index.TABLE_SCHEMA}].[{index.TABLE_NAME}].{index.INDEX_NAME};");
        scriptBuilder.AppendLine("GO");
        scriptBuilder.AppendLine();
      }
    }

    private static string GetForeignKeyId(ForeignKey foreignKey)
    {
      return $"{foreignKey.REFERENCED_TABLE_NAME}|{foreignKey.REFERENCED_COLUMN_NAME}";
    }

    private static string GetColumnIndexId(ColumnIndex columnIndex)
    {
      return $"{columnIndex.TABLE_NAME}|{columnIndex.COLUMN_NAME}";
    }

    private static void GenerateTableCreation(Table destinationTable, StringBuilder scriptBuilder)
    {
      scriptBuilder.AppendLine($"CREATE TABLE [{destinationTable.Name}]");
      var columns = destinationTable.Columns.Values.ToList();
      for (int i = 0; i < columns.Count; i++)
      {
        var column = columns[i];
        GenerateAddColumnScript(column, scriptBuilder, columns.IsLast(i));
      }
      scriptBuilder.AppendLine("GO");

      scriptBuilder.AppendLine($"ALTER TABLE {destinationTable.Name} ADD CONSTRAINT PK_{destinationTable.Name} PRIMARY KEY ({string.Join(", ", destinationTable.PrimaryKeys)})");
      scriptBuilder.AppendLine("GO");
      scriptBuilder.AppendLine();

      GenerateForeignKeyUpdates(null, destinationTable, scriptBuilder);
      GenerateIndexUpdates(null, destinationTable, scriptBuilder);
    }

    private static void GenerateTableDeletion(Table sourceTable, StringBuilder scriptBuilder)
    {
      scriptBuilder.AppendLine($"-- deleting table {sourceTable.Name}");
      scriptBuilder.AppendLine($"DROP TABLE [{sourceTable.Name}];");
      scriptBuilder.AppendLine("GO");
      scriptBuilder.AppendLine();
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

    public List<ColumnIndex> Indices = new List<ColumnIndex>();
  }

  internal class ColumnIndex
  {
    public string TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, INDEX_NAME;
    public bool IS_UNIQUE, IS_NONCLUSTERED;
  }

  internal class ForeignKey
  {
    public string FK_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, UPDATE_CASCADE_ACTION, DELETE_CASCADE_ACTION;
  }
}