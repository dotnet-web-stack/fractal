using System.Buffers;

namespace Fractal.Utils;

// MDA2AV Note:
//
// Taken directly from Minima

/// <summary>
/// One segment of a multi-buffer ReadOnlySequence&lt;byte&gt; built by ConnectionPipeReader when a
/// read spans more than one recv buffer. BufferId is carried for debugging only.
/// </summary>
public sealed class RingSegment : ReadOnlySequenceSegment<byte>
{
    public ushort BufferId { get; }

    public RingSegment(ReadOnlyMemory<byte> memory, ushort bufferId)
    {
        Memory = memory;
        BufferId = bufferId;
    }

    public RingSegment Append(ReadOnlyMemory<byte> memory, ushort bufferId)
    {
        var next = new RingSegment(memory, bufferId)
        {
            RunningIndex = RunningIndex + Memory.Length
        };

        Next = next;
        return next;
    }
}
