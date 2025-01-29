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
    var entities = new List<FakeTableEntity> { new() { PartitionKey = "pk", RowKey = "rk" } };
    creator.SetTableData("test", entities);

    // Act
    var tableClient = creator.CreateTableClient("test", "connectionString");

    // Assert
    var items = tableClient.Query<FakeTableEntity>().ToList();
    Assert.That(items, Is.Not.Null);
    Assert.That(items, Has.Exactly(1).Items);
    using (Assert.EnterMultipleScope())
    {
      Assert.That(items[0].PartitionKey, Is.EqualTo("pk"));
      Assert.That(items[0].RowKey, Is.EqualTo("rk"));
    }
  }
}
