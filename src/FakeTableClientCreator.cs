using Azure.Data.Tables;
using System.Collections;
using System.Collections.Generic;

namespace VectorCode.Azure.TableStorage.Testing;

/// <summary>
/// Fake implementation of ITableClientCreator that allows setting initial data for tables and uses a Mocked TableClient
/// </summary>
public class FakeTableClientCreator : ITableClientCreator
{
  private readonly Dictionary<string, List<ITableEntity>> _initialTables = [];

  /// <summary>
  /// Sets the initial data for a table
  /// </summary>
  /// <param name="tableName"></param>
  /// <param name="entities"></param>
  public void SetTableData<T>(string tableName, List<T> entities) where T : ITableEntity
  {
    _initialTables[tableName] = new List<ITableEntity>(entities.Cast<ITableEntity>());
  }

  /// <inheritdoc />
  public TableClient CreateTableClient(string tableName, string connectionString)
  {
    var setup = _initialTables[tableName];
    if(setup == default)
    {
      throw new ArgumentException($"No initial data set for table {tableName}");
    }
    var fakeTableClientType = typeof(FakeTableClient);
    var fake = Activator.CreateInstance(fakeTableClientType, tableName, new List<ITableEntity>(setup)) as TableClient;
    return fake!;
  }
}
