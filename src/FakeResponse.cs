using Azure;
using Azure.Core;
using System.Diagnostics.CodeAnalysis;

namespace VectorCode.Azure.TableStorage.Testing;

/// <summary>
/// A fake implementation of Response to fake call responses to Azure services
/// </summary>
[ExcludeFromCodeCoverage]
public sealed class FakeResponse : Response
{
  /// <inheritdoc />
  public override int Status => throw new NotImplementedException();

  /// <inheritdoc />
  public override string ReasonPhrase => throw new NotImplementedException();

  /// <inheritdoc />
  public override Stream? ContentStream
  {
    get => throw new NotImplementedException();
    set => throw new NotImplementedException();
  }

  /// <inheritdoc />
  public override string ClientRequestId
  {
    get => throw new NotImplementedException();
    set => throw new NotImplementedException();
  }

  /// <inheritdoc />
  public override void Dispose() =>
      throw new NotImplementedException();

  /// <inheritdoc />
  protected override bool ContainsHeader(string name) =>
      throw new NotImplementedException();

  /// <inheritdoc />
  protected override IEnumerable<HttpHeader> EnumerateHeaders() =>
      throw new NotImplementedException();

  /// <inheritdoc />
  protected override bool TryGetHeader(
      string name,
      [NotNullWhen(true)] out string? value) =>
      throw new NotImplementedException();

  /// <inheritdoc />
  protected override bool TryGetHeaderValues(
      string name,
      [NotNullWhen(true)] out IEnumerable<string>? values) =>
      throw new NotImplementedException();
}