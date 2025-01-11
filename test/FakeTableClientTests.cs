using Azure;
using Azure.Data.Tables;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Security.Policy;
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
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Assert
      var partitions = fakeTableClient.Table.ToList();
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table.Keys.Should().Contain("pk1");
      var entities = fakeTableClient.Table["pk1"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk1");
      entities["rk1"].PartitionKey.Should().Be("pk1");
      entities["rk1"].RowKey.Should().Be("rk1");
    }

    [Test]
    public async Task FakeTableClient_CreateIfNotExistsAsync_ShouldReturnTableItemResponse()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      var result = await fakeTableClient.CreateIfNotExistsAsync();

      // Assert
      result.Value.Name.Should().Be("FakeTable");
    }

    [Test]
    public async Task FakeTableClient_GetEntityAsync_ShouldReturnExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      var result = await fakeTableClient.GetEntityAsync<FakeTableEntity>("pk1", "rk1");

      // Assert
      result.Value.PartitionKey.Should().Be("pk1");
      result.Value.RowKey.Should().Be("rk1");
    }

    [Test]
    public async Task FakeTableClient_GetEntityAsync_ShouldThrowForNonExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      var act = () => fakeTableClient.GetEntityAsync<FakeTableEntity>("pk1", "rk2");

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
    }

    [Test]
    public async Task FakeTableClient_GetEntityIfExistsAsync_ShouldReturnExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      var result = await fakeTableClient.GetEntityIfExistsAsync<FakeTableEntity>("pk1", "rk1");

      // Assert
      result.HasValue.Should().BeTrue();
      result.Value.Should().NotBeNull();
      result.Value!.PartitionKey.Should().Be("pk1");
      result.Value.RowKey.Should().Be("rk1");
    }

    [Test]
    public async Task FakeTableClient_GetEntityIfExistsAsync_ShouldNotThrowForNonExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      NullableResponse<FakeTableEntity> result = new FakeNullableResponse<FakeTableEntity>(new FakeTableEntity { PartitionKey = "pk3", RowKey = "rk3" });
      var act = async () => result = await fakeTableClient.GetEntityIfExistsAsync<FakeTableEntity>("pk1", "rk2");

      // Assert
      await act.Should().NotThrowAsync<RequestFailedException>();
      result.HasValue.Should().BeFalse();
    }

    [Test]
    public async Task FakeTableClient_AddEntityAsync_ShouldAddEntity_AndSetETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.AddEntityAsync(newEntity);

      // Assert
      fakeTableClient.Table.Should().HaveCount(2);
      fakeTableClient.Table.Keys.Should().Contain("pk2");
      var entities = fakeTableClient.Table["pk2"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk2");
      entities["rk2"].PartitionKey.Should().Be("pk2");
      entities["rk2"].RowKey.Should().Be("rk2");
      entities["rk2"].Value.Should().Be("Value2");
      entities["rk2"].ETag.Should().NotBe(new ETag());
    }

    [Test]
    public async Task FakeTableClient_AddEntityAsync_ShouldThrowForBadType()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeOtherTableEntity { PartitionKey = "pk2", RowKey = "rk2", Value = 5 };

      // Act
      var act = () => fakeTableClient.AddEntityAsync(newEntity);

      // Assert
      await act.Should().ThrowAsync<ArgumentException>();
    }

    [Test]
    public async Task FakeTableClient_AddEntityAsync_ShouldThrowForDuplicateKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value2"};

      // Act
      var act = () => fakeTableClient.AddEntityAsync(newEntity);

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldUpdateEntity_AndChangeETag_WhenETagAll()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated", ETag = new ETag("123") };

      // Act
      await fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table.Keys.Should().Contain("pk1");
      var entities = fakeTableClient.Table["pk1"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk1");
      entities["rk1"].PartitionKey.Should().Be("pk1");
      entities["rk1"].RowKey.Should().Be("rk1");
      entities["rk1"].Value.Should().Be("updated");
      entities["rk1"].ETag.Should().NotBe(new ETag("123"));
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldUpdateEntity_AndChangeETag_WhenMatchingETagParam()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated", ETag = new ETag("") };

      // Act
      await fakeTableClient.UpdateEntityAsync(updatedEntity, new ETag("123"));

      // Assert
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table.Keys.Should().Contain("pk1");
      var entities = fakeTableClient.Table["pk1"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk1");
      entities["rk1"].PartitionKey.Should().Be("pk1");
      entities["rk1"].RowKey.Should().Be("rk1");
      entities["rk1"].Value.Should().Be("updated");
      entities["rk1"].ETag.Should().NotBe(new ETag("123"));
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldThrowForBadType()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeOtherTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = 2, ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      await act.Should().ThrowAsync<ArgumentException>();
    }


    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldThrowForWrongETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value2", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, new ETag("1234"));

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldSkipForMissingPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1", Value = "updated" };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      await act.Should().NotThrowAsync();
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table["pk1"].Should().HaveCount(1);
      fakeTableClient.Table["pk1"].Should().ContainKey("rk1");
      fakeTableClient.Table["pk1"]["rk1"].Value.Should().Be("Value");
    }

    [Test]
    public async Task FakeTableClient_UpdateEntityAsync_ShouldSkipForMissingRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "updated" };

      // Act
      var act = () => fakeTableClient.UpdateEntityAsync(updatedEntity, ETag.All);

      // Assert
      await act.Should().NotThrowAsync();
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table["pk1"].Should().HaveCount(1);
      fakeTableClient.Table["pk1"].Should().ContainKey("rk1");
      fakeTableClient.Table["pk1"]["rk1"].Value.Should().Be("Value");
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldUpdateExistingEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var updatedEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "updated" };

      // Act
      await fakeTableClient.UpsertEntityAsync(updatedEntity);

      // Assert
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table.Keys.Should().Contain("pk1");
      var entities = fakeTableClient.Table["pk1"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk1");
      entities["rk1"].PartitionKey.Should().Be("pk1");
      entities["rk1"].RowKey.Should().Be("rk1");
      entities["rk1"].Value.Should().Be("updated");
      entities["rk1"].ETag.Should().NotBe(new ETag("123"));
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldAddNewEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.UpsertEntityAsync(newEntity);

      // Assert
      fakeTableClient.Table.Should().HaveCount(1);
      var entities = fakeTableClient.Table["pk1"];
      entities.Should().HaveCount(2);
      entities.Keys.Should().Contain("rk2");
      entities["rk2"].PartitionKey.Should().Be("pk1");
      entities["rk2"].RowKey.Should().Be("rk2");
      entities["rk2"].Value.Should().Be("Value2");
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldAddNewEntityWithNewPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk2", Value = "Value2" };

      // Act
      await fakeTableClient.UpsertEntityAsync(newEntity);

      // Assert
      fakeTableClient.Table.Should().HaveCount(2);
      fakeTableClient.Table.Keys.Should().Contain("pk2");
      var entities = fakeTableClient.Table["pk2"];
      entities.Should().HaveCount(1);
      entities.Keys.Should().Contain("rk2");
      entities["rk2"].PartitionKey.Should().Be("pk2");
      entities["rk2"].RowKey.Should().Be("rk2");
      entities["rk2"].Value.Should().Be("Value2");
    }

    [Test]
    public async Task FakeTableClient_UpsertEntityAsync_ShouldThrowForBadType()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var newEntity = new FakeOtherTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = 2 };

      // Act
      var act = () => fakeTableClient.UpsertEntityAsync(newEntity);

      // Assert
      await act.Should().ThrowAsync<ArgumentException>();
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByKeys_ShouldDeleteEntity()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      await fakeTableClient.DeleteEntityAsync("pk1", "rk1", ETag.All);

      // Assert
      fakeTableClient.Table["pk1"].Should().BeEmpty();
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByEntity_ShouldDeleteEntity()
    {
      // Arrange
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("") };
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });

      // Act
      await fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      fakeTableClient.Table["pk1"].Should().BeEmpty();
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByEntity_ShouldNotThrowErrorForNotFoundPartition()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var entity = new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1", Value = "Value", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      await act.Should().NotThrowAsync();
      fakeTableClient.Table["pk1"].Should().HaveCount(1);
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByEntity_ShouldNotThrowErrorForNotFoundRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value", ETag = new ETag("123") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, ETag.All);

      // Assert
      await act.Should().NotThrowAsync();
      fakeTableClient.Table["pk1"].Should().HaveCount(1);
    }

    [Test]
    public async Task FakeTableClient_DeleteEntityAsyncByEntity_ShouldThrowErrorDifferentETag()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("123") }
      });
      var entity = new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value", ETag = new ETag("1234") };

      // Act
      var act = () => fakeTableClient.DeleteEntityAsync(entity, entity.ETag);

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
      fakeTableClient.Table["pk1"].Should().HaveCount(1);
    }

    [Test]
    public async Task FakeTableClient_QueryAsync_ShouldFetchAllRecordsForPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") },
        new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk5", Value = "Value5", ETag = new ETag("1235") }
      });

      // Act
      var result = fakeTableClient.QueryAsync<FakeTableEntity>(q => q.PartitionKey == "pk1");

      // Assert
      var list = await result.ToListAsync();
      list.Should().HaveCount(4);
    }

    [Test]
    public async Task FakeTableClient_QueryAsync_ShouldFetchOneRecordForPartitionKeyAndRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });

      // Act
      var result = fakeTableClient.QueryAsync<FakeTableEntity>(q => q.PartitionKey == "pk1" && q.RowKey == "rk1");

      // Assert
      var list = await result.ToListAsync();
      list.Should().HaveCount(1);
      list.First().Value.Should().Be("Value1");
    }

    [Test]
    public void FakeTableClient_Query_ShouldFetchAllRecordsForPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") },
        new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk5", Value = "Value5", ETag = new ETag("1235") }
      });

      // Act
      var result = fakeTableClient.Query<FakeTableEntity>(q => q.PartitionKey == "pk1");

      // Assert
      var list = result.ToList();
      list.Should().HaveCount(4);
    }

    [Test]
    public void FakeTableClient_Query_ShouldFetchOneRecordForPartitionKeyAndRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });

      // Act
      var result = fakeTableClient.Query<FakeTableEntity>(q => q.PartitionKey == "pk1" && q.RowKey == "rk1");

      // Assert
      var list = result.ToList();
      list.Should().HaveCount(1);
      list.First().Value.Should().Be("Value1");
    }

    [Test]
    public async Task FakeTableClient_SubmitTransactionAsync_ShouldDoAllChanges_WhenAllValid()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });
      var transaction = new List<TableTransactionAction>
      {
        new TableTransactionAction(TableTransactionActionType.Add, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk5", Value = "Value5" }, ETag.All),
        new TableTransactionAction(TableTransactionActionType.UpdateReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4-u" }, ETag.All ),
        new TableTransactionAction(TableTransactionActionType.UpsertReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3-u" }, ETag.All),
        new TableTransactionAction(TableTransactionActionType.UpsertReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk6", Value = "Value6" }, ETag.All),
        new TableTransactionAction(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1" }, ETag.All),
      };

      // Act
      await fakeTableClient.SubmitTransactionAsync(transaction);


      // Assert
      fakeTableClient.Table.Should().HaveCount(1);
      fakeTableClient.Table["pk1"].Should().HaveCount(5);
      fakeTableClient.Table["pk1"].Should().ContainKey("rk5");
      fakeTableClient.Table["pk1"].Should().ContainKey("rk6");
      fakeTableClient.Table["pk1"]["rk4"].Value.Should().Be("Value4-u");
      fakeTableClient.Table["pk1"]["rk3"].Value.Should().Be("Value3-u");
      fakeTableClient.Table["pk1"].Should().NotContainKey("rk1");
    }

    [Test]
    public async Task FakeTableClient_SubmitTransactionAsync_ShouldThrowForDeleteOfMissingPartitionKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });
      var transaction = new List<TableTransactionAction>
      {
        new TableTransactionAction(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk2", RowKey = "rk1" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
    }

    [Test]
    public async Task FakeTableClient_SubmitTransactionAsync_ShouldThrowForDeleteOfMissingRowKey()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });
      var transaction = new List<TableTransactionAction>
      {
        new TableTransactionAction(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk12" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
    }

    [Test]
    public async Task FakeTableClient_SubmitTransactionAsync_ShouldRollbackWhenErrorThrown()
    {
      // Arrange
      var fakeTableClient = new FakeTableClient<FakeTableEntity>("FakeTable", new List<FakeTableEntity>
      {
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1", ETag = new ETag("1231") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2", Value = "Value2", ETag = new ETag("1232") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk3", Value = "Value3", ETag = new ETag("1233") },
        new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk4", Value = "Value4", ETag = new ETag("1234") }
      });
      var transaction = new List<TableTransactionAction>
      {
        new TableTransactionAction(TableTransactionActionType.UpdateReplace, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk1", Value = "Value1-u" }, ETag.All),
         new TableTransactionAction(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk2" }, ETag.All),
        new TableTransactionAction(TableTransactionActionType.Delete, new FakeTableEntity { PartitionKey = "pk1", RowKey = "rk12" }, ETag.All),
      };

      // Act
      var act = () => fakeTableClient.SubmitTransactionAsync(transaction);

      // Assert
      await act.Should().ThrowAsync<RequestFailedException>();
      fakeTableClient.Table["pk1"].Should().HaveCount(4);
      fakeTableClient.Table["pk1"]["rk1"].Value.Should().Be("Value1");
      fakeTableClient.Table["pk1"]["rk1"].ETag.Should().Be(new ETag("1231"));
    }





  }
}