using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using System.Linq.Expressions;

namespace VectorCode.Azure.TableStorage.Testing;

/// <summary>
/// A fake implementation of TableClient that stores data in memory.  Note, certain functionality does not work, including:
///  - the select parameter in GetEntity  is ignored, the full entity is returned
///  - merge actions will do a full replace
///  - query with odata string will throw a not implemented exception
///  - query max per page will only generate one page
/// </summary>
/// <typeparam name="T"></typeparam>
public class FakeTableClient<T> : TableClient where T : BaseTableEntity, ITableEntity
{
  /// <summary>
  /// The table data
  /// </summary>
  public Dictionary<string, Dictionary<string, T>> Table { get; private set; }
  private readonly string _name;

  /// <summary>
  /// Creates a new FakeTableClient with the given initial data
  /// </summary>
  /// <param name="name"></param>
  /// <param name="initialData"></param>
  public FakeTableClient(string name, List<T> initialData) : base()
  {
    _name = name;
    var table = new Dictionary<string, Dictionary<string, T>>();
    foreach (var entity in initialData)
    {
      if (!table.TryGetValue(entity.PartitionKey, out _))
      {
        table[entity.PartitionKey] = [];
      }
      table[entity.PartitionKey][entity.RowKey] = entity;
    }
    Table = table;
  }

  /// <inheritdoc />
  public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
  {
    return Response.FromValue(new TableItem(_name), new FakeResponse());
  }

  /// <inheritdoc />
  public override Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
  {
    return Task.FromResult(CreateIfNotExists(cancellationToken));
  }

  /// <inheritdoc />
  public override Response<TL> GetEntity<TL>(string partitionKey, string rowKey, IEnumerable<string>? select = default, CancellationToken cancellationToken = default)
    => Table.TryGetValue(partitionKey, out var subSet) && subSet.TryGetValue(rowKey, out var value)
                    ? Response.FromValue((value as TL)!, new FakeResponse())
                    : throw new RequestFailedException(404, "Not Found");

  /// <inheritdoc />
  public override NullableResponse<TL> GetEntityIfExists<TL>(string partitionKey, string rowKey, IEnumerable<string>? select = default, CancellationToken cancellationToken = default)
  {
    return Table.TryGetValue(partitionKey, out var subSet) && subSet.TryGetValue(rowKey, out var value)
                    ? new FakeNullableResponse<TL>(value as TL)
                    : new FakeNullableResponse<TL>(null);
  }

  /// <inheritdoc />
  public override Task<Response<TL>> GetEntityAsync<TL>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(GetEntity<TL>(partitionKey, rowKey, select, cancellationToken));
  }

  /// <inheritdoc />
  public override Task<NullableResponse<TL>> GetEntityIfExistsAsync<TL>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(GetEntityIfExists<TL>(partitionKey, rowKey, select, cancellationToken));
  }

  /// <inheritdoc />
  public override Response AddEntity<TL>(TL entity, CancellationToken cancellationToken = default)
  {
    AddEntity(entity);
    return new FakeResponse();
  }

  /// <inheritdoc />
  public override Task<Response> AddEntityAsync<TL>(TL entity, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(AddEntity(entity, cancellationToken));
  }

  /// <inheritdoc />
  public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
  {
    var tableClone = ShallowCloneTable();
    try
    {
      transactionActions.ToList().ForEach(action =>
      {
        var entity = action.Entity;
        switch (action.ActionType)
        {
          case TableTransactionActionType.Add:
            AddEntity(entity);
            break;
          case TableTransactionActionType.Delete:
            DeleteEntity(entity.PartitionKey, entity.RowKey, true, action.ETag);
            break;
          case TableTransactionActionType.UpdateReplace:
          case TableTransactionActionType.UpdateMerge:
            UpdateEntity(entity, action.ETag);
            break;
          case TableTransactionActionType.UpsertMerge:
          case TableTransactionActionType.UpsertReplace:
            UpsertEntity(entity);
            break;
          default:
            throw new ArgumentException("Unknown action type");
        }
      });
      return Response.FromValue(transactionActions.Select(action => new FakeResponse()).ToList() as IReadOnlyList<Response>, new FakeResponse());
    }
    catch
    {
      Table = tableClone;
      throw;
    }
  }

  /// <inheritdoc />
  public override Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(SubmitTransaction(transactionActions, cancellationToken));
  }

  /// <inheritdoc />
  public override Response UpdateEntity<TL>(TL entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
  {
    UpdateEntity(entity, ifMatch);
    return new FakeResponse();
  }

  /// <inheritdoc />
  public override Task<Response> UpdateEntityAsync<TL>(TL entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(UpdateEntity(entity, ifMatch, mode, cancellationToken));
  }

  /// <inheritdoc />
  public override Response UpsertEntity<TL>(TL entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
  {
    UpsertEntity(entity);
    return new FakeResponse();
  }

  /// <inheritdoc />
  public override Task<Response> UpsertEntityAsync<TL>(TL entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(UpsertEntity(entity, mode, cancellationToken));
  }

  /// <inheritdoc />
  public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
  {
    DeleteEntity(entity.PartitionKey, entity.RowKey, false, ifMatch);
    return new FakeResponse();
  }

  /// <inheritdoc />
  public override Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(DeleteEntity(entity, ifMatch, cancellationToken));
  }

  /// <inheritdoc />
  public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
  {
    DeleteEntity(partitionKey, rowKey, false, ifMatch);
    return new FakeResponse();
  }

  /// <inheritdoc />
  public override Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
  {
    return Task.FromResult(DeleteEntity(partitionKey, rowKey, ifMatch, cancellationToken));
  }

  /// <inheritdoc />
  public override Pageable<TL> Query<TL>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    if (!string.IsNullOrEmpty(filter)) { 
      throw new NotImplementedException();
    }
    return Query<TL>(x => true, maxPerPage, select, cancellationToken);
  }

  /// <inheritdoc />
  public override AsyncPageable<TL> QueryAsync<TL>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    if (!string.IsNullOrEmpty(filter))
    {
      throw new NotImplementedException();
    }
    return QueryAsync<TL>(x => true, maxPerPage, select, cancellationToken);
  }

  /// <inheritdoc />
  public override Pageable<TL> Query<TL>(Expression<Func<TL, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    var query = (Table.Values.SelectMany(x => x.Values).AsQueryable() as IQueryable<TL>)!;
    query = query.Where(filter);
    if (maxPerPage.HasValue)
    {
      query = query.Take(maxPerPage.Value);
    }
    return Pageable<TL>.FromPages([Page<TL>.FromValues(query.ToList().AsReadOnly(), null, new FakeResponse())]);
  }

  /// <inheritdoc />
  public override AsyncPageable<TL> QueryAsync<TL>(Expression<Func<TL, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
  {
    var query = (Table.Values.SelectMany(x => x.Values).AsQueryable() as IQueryable<TL>)!;
    query = query.Where(filter);
    if (maxPerPage.HasValue)
    {
      query = query.Take(maxPerPage.Value);
    }
    return AsyncPageable<TL>.FromPages([Page<TL>.FromValues(query.ToList().AsReadOnly(), null, new FakeResponse())]);
  }

  private void DeleteEntity(string partitionKey, string rowKey, bool throwIfNotFound, ETag ifMatch)
  {
    if (!Table.TryGetValue(partitionKey, out var subTable))
    {
      if(throwIfNotFound)
        throw new RequestFailedException(404, "Not Found");
      return;
    }
    if (!subTable.TryGetValue(rowKey, out T? value))
    {
      if(throwIfNotFound)
        throw new RequestFailedException(404, "Not Found");
      return;
    }
    if (ifMatch != ETag.All && value.ETag != ifMatch)
    {
      throw new RequestFailedException(412, "Precondition Failed");
    }
    subTable.Remove(rowKey);
  }

  private void AddEntity(ITableEntity entity)
  {
    if(entity is not T tEntity)
    {
      throw new ArgumentException("Entity is not of the correct type");
    }
    tEntity.ETag = new ETag(Guid.NewGuid().ToString());
    tEntity.Timestamp = DateTimeOffset.UtcNow;
    if (!Table.TryGetValue(entity.PartitionKey, out var subTable))
    {
      Table[entity.PartitionKey] = new Dictionary<string, T>();
      subTable = Table[entity.PartitionKey];
    }
    if (subTable.ContainsKey(entity.RowKey))
    {
      throw new RequestFailedException(409, "Conflict");
    }
    subTable[entity.RowKey] = tEntity;
  }

  private void UpdateEntity(ITableEntity entity, ETag ifMatch)
  {
    if (entity is not T tEntity)
    {
      throw new ArgumentException("Entity is not of the correct type");
    }
    if (!Table.TryGetValue(entity.PartitionKey, out var subTable))
    {
      return;
    }
    if (!subTable.TryGetValue(entity.RowKey, out T? value))
    {
      return;
    }
    if (ifMatch != ETag.All && value.ETag != ifMatch)
    {
      throw new RequestFailedException(412, "Precondition Failed");
    }
    tEntity.ETag = new ETag(Guid.NewGuid().ToString());
    tEntity.Timestamp = DateTimeOffset.UtcNow;
    subTable[entity.RowKey] = tEntity;    
  }

  private void UpsertEntity(ITableEntity entity)
  {
    if (entity is not T tEntity)
    {
      throw new ArgumentException("Entity is not of the correct type");
    }
    tEntity.ETag = new ETag(Guid.NewGuid().ToString());
    tEntity.Timestamp = DateTimeOffset.UtcNow;
    if (!Table.TryGetValue(entity.PartitionKey, out var subTable))
    {
      Table[entity.PartitionKey] = new Dictionary<string, T>();
      subTable = Table[entity.PartitionKey];
    }
    subTable[entity.RowKey] = tEntity;
  }

  /// <summary>
  /// Shallow clones the table
  /// </summary>
  /// <returns></returns>
  public Dictionary<string, Dictionary<string, T>> ShallowCloneTable()
  {
    var clone = new Dictionary<string, Dictionary<string, T>>(); 
    foreach (var outerPair in Table) 
    { 
      var innerClone = new Dictionary<string, T>(); 
      foreach (var innerPair in outerPair.Value) 
      { 
        innerClone.Add(innerPair.Key, (T)innerPair.Value); 
      }
      clone.Add(outerPair.Key, innerClone); 
    }
    return clone;
  }

}