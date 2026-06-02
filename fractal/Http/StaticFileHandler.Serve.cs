// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

public sealed partial class StaticFileHandler
{
    // The final header line is "Content-Length: <value>\r\n\r\n", a fixed ClLineWidth bytes: 16 for
    // the name, ClWidth for the value right-justified (a single-slab body needs at most 5 digits, 7
    // is slack), 4 for the blank line. Leading spaces in the value are valid header whitespace.
    private const int ClWidth     = 7;
    private const int ClLineWidth = 16 + ClWidth + 4;

    /// <summary>
    /// Serve a resolved asset. The body is read into the write slab and the Content-Length is taken
    /// from what we actually read (the io_uring read result), so a changed or stale file can never
    /// produce a wrong length. A body that fills the slab is streamed with Transfer-Encoding: chunked,
    /// since the total isn't known before the header goes out. <paramref name="prefix"/> is the baked
    /// header up to, but not including, the Content-Length / Transfer-Encoding line.
    /// </summary>
    private async Task ServeAsync(Connection conn, AssetCache.Asset asset, byte[] prefix)
    {
        // Lay down the prefix, then reserve a fixed gap for the final header line without writing it.
        // We don't yet know if that line is a Content-Length or a switch to chunked, the read tells us.
        // Reserving up front keeps the body contiguous with the header for a single send.
        conn.Write(prefix);
        int lineAt = conn.WriteTail;
        conn.WriteTail += ClLineWidth;        // skip the gap, its contents are decided below
        int headerLen = conn.WriteTail;

        int budget = conn.WriteSlabSize - headerLen;
        int n = await conn.ReadFileAsync(asset.Fd, budget, 0);

        if (n < budget)
        {
            // The whole file fit. Only now, knowing the length, do we write the Content-Length line.
            WriteContentLength(conn.WriteSlab.Slice(lineAt, ClLineWidth), n);
            conn.WriteTail = headerLen + n;
            await conn.FlushAsync();          // prefix + Content-Length + body, one send
            return;
        }

        // Too big to know the total up front. The gap is never written and never sent, stream chunked.
        await SendChunkedAsync(conn, asset, prefix);
    }

    // Fill exactly ClLineWidth bytes with "Content-Length: <value right-justified>\r\n\r\n".
    private static void WriteContentLength(Span<byte> line, int value)
    {
        "Content-Length: "u8.CopyTo(line);
        FormatRight(line.Slice(16, ClWidth), value);
        "\r\n\r\n"u8.CopyTo(line[(16 + ClWidth)..]);
    }

    private static async Task SendChunkedAsync(Connection conn, AssetCache.Asset asset, byte[] prefix)
    {
        conn.WriteTail = 0;
        conn.Write(prefix);
        conn.Write("Transfer-Encoding: chunked\r\n\r\n"u8);
        await conn.FlushAsync();

        const int Pfx = 8;                          // room at the front for the "<hex>\r\n" size line
        int budget = conn.WriteSlabSize - Pfx - 2;  // leave 2 bytes for the trailing CRLF
        long off = 0;
        while (true)
        {
            conn.WriteTail = Pfx;
            int dataLen = await conn.ReadFileAsync(asset.Fd, budget, off);
            if (dataLen == 0)
            {
                break;
            }
            off += dataLen;

            conn.WriteTail = FrameChunk(conn.WriteSlab, Pfx, dataLen);
            await conn.FlushAsync();

            if (dataLen < budget)
            {
                break;   // short read, that was the last chunk
            }
        }

        conn.WriteTail = 0;
        conn.Write("0\r\n\r\n"u8);
        await conn.FlushAsync();
    }

    // Lay one chunk out as "<hex>\r\n<data>\r\n". The data sits at dataOffset, we write the size line
    // at the front and slide the data up against it. Returns the total framed length.
    private static int FrameChunk(Span<byte> slab, int dataOffset, int dataLen)
    {
        int p = WriteChunkHeader(slab, dataLen);
        slab.Slice(dataOffset, dataLen).CopyTo(slab.Slice(p, dataLen));
        slab[p + dataLen]     = (byte)'\r';
        slab[p + dataLen + 1] = (byte)'\n';
        return p + dataLen + 2;
    }

    // Write value as lowercase hex followed by CRLF at the front of slab. Returns bytes written.
    private static int WriteChunkHeader(Span<byte> slab, int value)
    {
        Span<byte> hex = stackalloc byte[8];
        int i = hex.Length;
        do
        {
            int d = value & 0xF;
            hex[--i] = (byte)(d < 10 ? '0' + d : 'a' + d - 10);
            value >>= 4;
        }
        while (value != 0);

        int len = hex.Length - i;
        hex[i..].CopyTo(slab);
        slab[len]     = (byte)'\r';
        slab[len + 1] = (byte)'\n';
        return len + 2;
    }

    // Right-justify value (decimal) into field, padded with leading spaces.
    private static void FormatRight(Span<byte> field, int value)
    {
        field.Fill((byte)' ');
        int i = field.Length;
        do
        {
            field[--i] = (byte)('0' + value % 10);
            value /= 10;
        }
        while (value != 0 && i > 0);
    }
}
