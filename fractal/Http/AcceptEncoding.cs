using System.Globalization;
using System.Text;
using Glyph11.Protocol;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

/// <summary>Content codings the server can serve as pre-compressed variants.</summary>
[Flags]
internal enum AcceptEncoding
{
    None = 0,
    Gzip = 1 << 0,
    Br   = 1 << 1,
}

internal static class AcceptEncodingParser
{
    /// <summary>
    /// Parse an Accept-Encoding header honoring q-values (q=0 rejects an encoding). br is preferred
    /// over gzip downstream, so finer q-ordering isn't weighed. "*" is not expanded.
    /// </summary>
    public static AcceptEncoding Parse(BinaryRequest request)
    {
        AcceptEncoding accepted = AcceptEncoding.None;
        foreach (KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> header in request.Headers.AsSpan())
        {
            if (!Ascii.EqualsIgnoreCase(header.Key.Span, "accept-encoding"u8))
            {
                continue;
            }

            var headerValue = header.Value.Span;
            while (!headerValue.IsEmpty)
            {
                int comma = headerValue.IndexOf((byte)',');
                var token = comma < 0 ? headerValue : headerValue[..comma];
                headerValue = comma < 0 ? default : headerValue[(comma + 1)..];

                int semi = token.IndexOf((byte)';');
                var name = (semi < 0 ? token : token[..semi]).Trim((byte)' ');
                if (semi >= 0 && Qvalue(token[(semi + 1)..]) <= 0d)
                {
                    continue;   // q=0 -> explicitly not acceptable
                }

                if (Ascii.EqualsIgnoreCase(name, "br"u8))
                {
                    accepted |= AcceptEncoding.Br;
                }
                else if (Ascii.EqualsIgnoreCase(name, "gzip"u8))
                {
                    accepted |= AcceptEncoding.Gzip;
                }
            }
            break;
        }
        return accepted;
    }

    // q-value from a coding's parameters ("...;q=0.8" -> 0.8). Defaults to 1 when absent/unparseable.
    private static double Qvalue(ReadOnlySpan<byte> parameters)
    {
        int qi = parameters.IndexOf("q="u8);
        if (qi < 0)
        {
            return 1d;
        }
        
        var num = parameters[(qi + 2)..];
        int end = 0;
        
        while (end < num.Length && (num[end] == (byte)'.' || (uint)(num[end] - (byte)'0') <= 9))
        {
            end++;
        }
        
        return double.TryParse(num[..end], CultureInfo.InvariantCulture, out double q) ? q : 1d;
    }
}
