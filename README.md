# SqlUpdateScriptGenerator
This tool will connect to two MS SQL server instances, pull the metadata about tables and columns, and create a SQL update script to add, edit, or remove any tables and columns.

## Bugs
Please report any bugs on Github: https://github.com/caligo-erik/SqlUpdateScriptGenerator/issues

## Usage

The basic usage is as follows, the output path is optional:

```bash
SqlGenerateUpdateScript -s "sourceConnectionString" -d "destinationConnectionString" -o "outputFilePath"
```
