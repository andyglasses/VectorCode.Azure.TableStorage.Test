namespace VectorCode.Azure.TableStorage.Testing.Test.Fakes;

public record FakeTableEntity : BaseTableEntity
{
  public string? Value { get; set; }
}
