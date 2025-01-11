using Azure.Data.Tables;
using System.Collections;

namespace VectorCode.Azure.TableStorage.Testing;

/// <summary>
/// Fake implementation of ITableClientCreator that allows setting initial data for tables and uses a Mocked TableClient
/// </summary>
public class FakeTableClientCreator : ITableClientCreator
{
  private readonly Dictionary<string, (Type, object)> _initialTables = [];

  /// <summary>
  /// Sets the initial data for a table
  /// </summary>
  /// <typeparam name="T"></typeparam>
  /// <param name="tableName"></param>
  /// <param name="entities"></param>
  public void SetTableData<T>(string tableName, List<T> entities) where T : BaseTableEntity
  {
    _initialTables[tableName] = (typeof(T), entities);
  }

  /// <inheritdoc />
  public TableClient CreateTableClient(string tableName, string connectionString)
  {
    var setup = _initialTables[tableName];
    if(setup == default)
    {
      throw new ArgumentException($"No initial data set for table {tableName}");
    }
    Type tableType = setup.Item1;
    Type listType = typeof(List<>).MakeGenericType(tableType);
    var list = Activator.CreateInstance(listType);
    var objectList = setup.Item2 as IEnumerable;
    foreach (var item in objectList!)
    {
      listType.GetMethod("Add")?.Invoke(list, [item]);
    }
    var fakeTableClientType = typeof(FakeTableClient<>).MakeGenericType(tableType);
    var fake = Activator.CreateInstance(fakeTableClientType, tableName, list) as TableClient;
    return fake!;
  }
}
