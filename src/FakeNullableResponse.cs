using Azure;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VectorCode.Azure.TableStorage.Testing;

/// <summary>
/// A fake implementation of NullableResponse to fake call responses to Azure services
/// </summary>
/// <typeparam name="T"></typeparam>
[ExcludeFromCodeCoverage]
public class FakeNullableResponse<T> : NullableResponse<T>
{
  private readonly T? _value;

  /// <summary>
  /// Creates a new FakeNullableResponse with the given value
  /// </summary>
  /// <param name="value"></param>
  public FakeNullableResponse(T? value)
  {
    _value = value;
  }



  /// <inheritdoc />
  public override bool HasValue => _value != null;

  /// <inheritdoc />
  public override T? Value => _value;

  /// <inheritdoc />
  public override Response GetRawResponse()
  {
    throw new NotImplementedException();
  }
}
