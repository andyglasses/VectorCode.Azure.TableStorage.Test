using Azure;
using Azure.Data.Tables;
using VectorCode.Azure.TableStorage.Testing.Test.Fakes;
using VectorCode.Common.Async;

namespace VectorCode.Azure.TableStorage.Testing.Test
{
  public class FakeTableClientTests
  {
    [Test]
    public void FakeTableClient_Constructor_ShouldInitializeData()
    {
      // Arrange & Act
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123"), Timestamp = DateTimeOffset.UtcNow }
      ]);

      // Assert
      var partitions = fakeTableClient.Table.ToList();
      Assert.That(partitions, Has.Exactly(1).Items);
      Assert.That(partitions[0].Key, Is.EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"].ToList();
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities[0].Key, Is.EqualTo("rk1"));
        Assert.That(entities[0].Value.PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities[0].Value.RowKey, Is.EqualTo("rk1"));
      }
    }

    [Test]
    public async Task FakeTableClient_CreateIfNotExistsAsync_ShouldReturnTableItemResponse()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      var result = await fakeTableClient.CreateIfNotExistsAsync();

      // Assert
      Assert.That(result.Value.Name, Is.EqualTo("FakeTable"));
    }

    [Test]
    public async Task FakeTableClient_GetEntityAsync_ShouldReturnExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      var result = await fakeTableClient.GetEntityAsync<FakeTableEntity>("pk1", "rk1");

      // Assert
      using (Assert.EnterMultipleScope())
      {
        Assert.That(result.Value.PartitionKey, Is.EqualTo("pk1"));
        Assert.That(result.Value.RowKey, Is.EqualTo("rk1"));
      }
    }

    [Test]
    public void FakeTableClient_GetEntityAsync_ShouldThrowForNonExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      var act = () => fakeTableClient.GetEntityAsync<FakeTableEntity>("pk1", "rk2");

      // Assert
      Assert.That(act, Throws.Exception.TypeOf<RequestFailedException>());
    }

    [Test]
    public async Task FakeTableClient_GetEntityIfExistsAsync_ShouldReturnExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      var result = await fakeTableClient.GetEntityIfExistsAsync<FakeTableEntity>("pk1", "rk1");

      using (Assert.EnterMultipleScope())
      {
        // Assert
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.Not.Null);
        Assert.That(result.Value?.PartitionKey, Is.EqualTo("pk1"));
        Assert.That(result.Value?.RowKey, Is.EqualTo("rk1"));
      }
    }

    [Test]
    public void FakeTableClient_GetEntityIfExistsAsync_ShouldNotThrowForNonExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable", [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      NullableResponse<FakeTableEntity> result = new FakeNullableResponse<FakeTableEntity>(new FakeTableEntity { PartitionKey = "pk3", RowKey = "rk3" });
      var act = async () => result = await fakeTableClient.GetEntityIfExistsAsync<FakeTableEntity>("pk1", "rk2");

      // Assert
      Assert.DoesNotThrowAsync(async () => await act());
      Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task FakeTableClient_AddEntityAsync_ShouldAddEntity_AndSetETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var newEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.AddEntityAsync(newEntity);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(2).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk2"));
      var entities = fakeTableClient.Table["pk2"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk2"));
        Assert.That(entities["rk2"].PartitionKey, Is.EqualTo("pk2"));
        Assert.That(entities["rk2"].RowKey, Is.EqualTo("rk2"));
        Assert.That((entities["rk2"] as FakeTableEntity)!.Value, Is.EqualTo("Value2"));
        Assert.That(entities["rk2"].ETag, Is.Not.EqualTo(new ETag()));
      }
    }

    [Test]
    public void FakeTableClient_AddEntityAsync_ShouldThrowForDuplicateKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var newEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value2"};

      // Act
      var act = () => fakeTableClient.AddEntityAsync(newEntity);

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldUpdateEntity_AndChangeETagAndTimestamp_WhenETagAll()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123"), Timestamp = new DateTimeOffset(new DateTime(2020,05,06, 14,11,8)) }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated", ETag = new ETag("123") };

      // Act
      await fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
        Assert.That(entities["rk1"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk1"].RowKey, Is.EqualTo("rk1"));
        Assert.That((entities["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("updated"));
        Assert.That(entities["rk1"].ETag, Is.Not.EqualTo(new ETag("123")));
        Assert.That(entities["rk1"].Timestamp, Is.GreaterThan(new DateTimeOffset(new DateTime(2020, 05, 06, 14, 11, 8))));
      }
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldUpdateEntity_AndChangeETagAndTimestamp_WhenMatchingETagParam()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123"), Timestamp = new DateTimeOffset(new DateTime(2020,05,06, 14,11,8)) }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated", ETag = new ETag("") };

      // Act
      await fakeTableClient.UpdateEntityAsync(updatedEntity, new ETag("123"));

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
        Assert.That(entities["rk1"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk1"].RowKey, Is.EqualTo("rk1"));
        Assert.That((entities["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("updated"));
        Assert.That(entities["rk1"].ETag, Is.Not.EqualTo(new ETag("123")));
        Assert.That(entities["rk1"].Timestamp, Is.GreaterThan(new DateTimeOffset(new DateTime(2020, 05, 06, 14, 11, 8))));
      }
    }

    [Test]
    public void FakeTableClient_UpdateEntityAsync_ShouldThrowForWrongETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value2", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, new ETag("1234"));

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
    }

    [Test]
    public void FakeTableClient_UpdateEntityAsync_ShouldSkipForMissingPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1", Value = "updated" };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      Assert.DoesNotThrowAsync(async () => await act());
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
        Assert.That(entities["rk1"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk1"].RowKey, Is.EqualTo("rk1"));
        Assert.That((entities["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("Value"));
      }
    }

    [Test]
    public void FakeTableClient_UpdateEntityAsync_ShouldSkipForMissingRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "updated" };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      Assert.DoesNotThrowAsync(async () => await act());
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
        Assert.That(entities["rk1"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk1"].RowKey, Is.EqualTo("rk1"));
        Assert.That((entities["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("Value"));
      }
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldUpdateExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123"), Timestamp = new DateTimeOffset(new DateTime(2020, 05, 12, 9, 14, 32)) }
      ]);
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated" };

      // Act
      await fakeTableClient.UpsertEntityAsync(updatedEntity);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
        Assert.That(entities["rk1"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk1"].RowKey, Is.EqualTo("rk1"));
        Assert.That((entities["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("updated"));
        Assert.That(entities["rk1"].ETag, Is.Not.EqualTo(new ETag("123")));
        Assert.That(entities["rk1"].Timestamp, Is.GreaterThan(new DateTimeOffset(new DateTime(2020, 05, 12, 9, 14, 32))));
      }
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldAddNewEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var newEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.UpsertEntityAsync(newEntity);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Has.Exactly(2).Items);
      Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk1"));
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk2"));
        Assert.That(entities["rk2"].PartitionKey, Is.EqualTo("pk1"));
        Assert.That(entities["rk2"].RowKey, Is.EqualTo("rk2"));
        Assert.That((entities["rk2"] as FakeTableEntity)!.Value, Is.EqualTo("Value2"));
      }
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldAddNewEntityWithNewPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var newEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.UpsertEntityAsync(newEntity);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(2).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk2"));
      var entities = fakeTableClient.Table["pk2"];
      Assert.That(entities, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(entities.Keys, Has.Exactly(1).EqualTo("rk2"));
        Assert.That(entities["rk2"].PartitionKey, Is.EqualTo("pk2"));
        Assert.That(entities["rk2"].RowKey, Is.EqualTo("rk2"));
        Assert.That((entities["rk2"] as FakeTableEntity)!.Value, Is.EqualTo("Value2"));
      }
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByKeys_ShouldDeleteEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      await fakeTableClient.DeleteEntityAsync("pk1", "rk1", ETag.All);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Is.Empty);
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByEntity_ShouldDeleteEntity()
    {
      // Arrange
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("") };
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);

      // Act
      await fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
      var entities = fakeTableClient.Table["pk1"];
      Assert.That(entities, Is.Empty);
    }

    [Test]
    public void FakeTableClient_DeleteEntityAsyncByEntity_ShouldNotThrowErrorForNotFoundPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var entity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1", Value = "Value", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      Assert.DoesNotThrowAsync(async () => await act());
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
    }

    [Test]
    public void FakeTableClient_DeleteEntityAsyncByEntity_ShouldNotThrowErrorForNotFoundRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      Assert.DoesNotThrowAsync(async () => await act());
      Assert.That(fakeTableClient.Table["pk1"], Has.Exactly(1).Items);
    }

    [Test]
    public void FakeTableClient_DeleteEntityAsyncByEntity_ShouldThrowErrorDifferentETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      ]);
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("1234") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, entity.ETag);

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
      Assert.That(fakeTableClient.Table["pk1"], Has.Exactly(1).Items);
    }

    [Test]
    public async Task FakeTableClient_QueryAsync_ShouldFetchAllRecordsForPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") },
        new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk5", Value = "Value5", ETag = new ETag("1235") }
      ]);

      // Act
      var result = fakeTableClient.QueryAsync<FakeTableEntity>(q => q.PartitionKey == "pk1");

      // Assert
      var list = await result.ToListAsync();
      Assert.That(list, Has.Exactly(4).Items);
    }

    [Test]
    public async Task FakeTableClient_QueryAsync_ShouldFetchOneRecordForPartitionKeyAndRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);

      // Act
      var result = fakeTableClient.QueryAsync<FakeTableEntity>(q => q.PartitionKey == "pk1" && q.RowKey == "rk1");

      // Assert
      var list = await result.ToListAsync();
      Assert.That(list, Has.Exactly(1).Items);
      Assert.That(list[0].Value, Is.EqualTo("Value1"));
    }

    [Test]
    public void FakeTableClient_Query_ShouldFetchAllRecordsForPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") },
        new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk5", Value = "Value5", ETag = new ETag("1235") }
      ]);

      // Act
      var result = fakeTableClient.Query<FakeTableEntity>(q => q.PartitionKey == "pk1");

      // Assert
      var list = result.ToList();
      Assert.That(list, Has.Exactly(4).Items);
    }

    [Test]
    public void FakeTableClient_Query_ShouldFetchOneRecordForPartitionKeyAndRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);

      // Act
      var result = fakeTableClient.Query<FakeTableEntity>(q => q.PartitionKey == "pk1" && q.RowKey == "rk1");

      // Assert
      var list = result.ToList();
      Assert.That(list, Has.Exactly(1).Items);
      Assert.That(list[0].Value, Is.EqualTo("Value1"));
    }

    [Test]
    public async Task FakeTableClient_SubmitTransactionAsync_ShouldDoAllChanges_WhenAllValid()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);
      var transaction = new List<TableTransactionAction>
      {
        new(TableTransactionActionType.Add, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk5", Value = "Value5" }, ETag.All),
        new(TableTransactionActionType.UpdateReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4-u" }, ETag.All ),
        new(TableTransactionActionType.UpsertReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3-u" }, ETag.All),
        new(TableTransactionActionType.UpsertReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk6", Value = "Value6" }, ETag.All),
        new(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1" }, ETag.All),
      };

      // Act
      await fakeTableClient.SubmitTransactionAsync(transaction);


      // Assert
      Assert.That(fakeTableClient.Table, Has.Exactly(1).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That(fakeTableClient.Table.Keys, Has.Exactly(1).EqualTo("pk1"));
        Assert.That(fakeTableClient.Table["pk1"], Has.Exactly(5).Items);
      }
      Assert.That(fakeTableClient.Table["pk1"].Keys, Has.Exactly(1).EqualTo("rk5"));
      using (Assert.EnterMultipleScope())
      {
        Assert.That(fakeTableClient.Table["pk1"].Keys, Has.Exactly(1).EqualTo("rk6"));
        Assert.That((fakeTableClient.Table["pk1"]["rk4"] as FakeTableEntity)!.Value, Is.EqualTo("Value4-u"));
        Assert.That((fakeTableClient.Table["pk1"]["rk3"] as FakeTableEntity)!.Value, Is.EqualTo("Value3-u"));
      }
      Assert.That(fakeTableClient.Table["pk1"].Keys, Has.None.EqualTo("rk1"));
    }

    [Test]
    public void FakeTableClient_SubmitTransactionAsync_ShouldThrowForDeleteOfMissingPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);
      var transaction = new List<TableTransactionAction>
      {
        new(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
    }

    [Test]
    public void FakeTableClient_SubmitTransactionAsync_ShouldThrowForDeleteOfMissingRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);
      var transaction = new List<TableTransactionAction>
      {
        new(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk12" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
    }

    [Test]
    public void FakeTableClient_SubmitTransactionAsync_ShouldRollbackWhenErrorThrown()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      ]);
      var transaction = new List<TableTransactionAction>
      {
        new(TableTransactionActionType.UpdateReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1-u" }, ETag.All),
        new(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2" }, ETag.All),
        new(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk12" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      Assert.ThrowsAsync<RequestFailedException>(async () => await act());
      Assert.That(fakeTableClient.Table["pk1"], Has.Exactly(4).Items);
      using (Assert.EnterMultipleScope())
      {
        Assert.That((fakeTableClient.Table["pk1"]["rk1"] as FakeTableEntity)!.Value, Is.EqualTo("Value1"));
        Assert.That(fakeTableClient.Table["pk1"]["rk1"].ETag, Is.EqualTo(new ETag("1231")));
      }
    }

    [Test]
    public async Task FakeTableClient_HandlesMultipleTypesInTable()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient("FakeTable",
      [
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeOtherTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = 22, ETag = new ETag("1232") },
      ]);

      // Act
      var result1 = await fakeTableClient.GetEntityIfExistsAsync<FakeTableEntity>("pk1", "rk1");
      var result2 = await fakeTableClient.GetEntityIfExistsAsync<FakeOtherTableEntity>("pk1", "rk2");

      // Assert      
      using (Assert.EnterMultipleScope())
      {
        Assert.That(result1.HasValue, Is.True);
        Assert.That(result1.Value!.Value, Is.EqualTo("Value1"));
        Assert.That(result2.HasValue, Is.True);
        Assert.That(result2.Value!.Value, Is.EqualTo(22));
      }
    }





  }
}