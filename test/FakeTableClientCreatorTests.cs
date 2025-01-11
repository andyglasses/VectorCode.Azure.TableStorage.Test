using FluentAssertions;
using VectorCode.Azure.TableStorage.Testing.Test.Fakes;

namespace VectorCode.Azure.TableStorage.Testing.Test;

[TestFixture]
public class FakeTableClientCreatorTests
{
  [Test]
  public void FakeTableClientCreator_CreateTableClient_WithInitialData()
  {
    // Arrange
    var creator = new FakeTableClientCreator();
    var entities = new List<FakeTableEntity> { new FakeTableEntity { PartitionKey = "pk", RowKey = "rk" } };
    creator.SetTableData("test", entities);

    // Act
    var tableClient = creator.CreateTableClient("test", "connectionString");

    // Assert
    var items = tableClient.Query<FakeTableEntity>().ToList();
    items.Should().NotBeNull();
    items.Should().HaveCount(1);
    items[0].PartitionKey.Should().Be("pk");
    items[0].RowKey.Should().Be("rk");
  }
}
