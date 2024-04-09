using MessagePack;
using System;
using System.Diagnostics;

namespace Utilities;

[MessagePackObject]
[Serializable]
public class GrainID : IEquatable<GrainID>
{
    [Key(0)]
    public readonly Guid id;

    [Key(1)]
    public readonly string className;

    public GrainID(Guid id, string className)
    {
        this.id = id;
        this.className = className;
    }

    public bool Equals(GrainID other) => other != null && id == other.id && className == other.className;

    public override int GetHashCode() => HashCode.Combine(id, className);

    public override string ToString() => id.ToString() + "+" + className;

    public static GrainID GetID(string s)
    {
        var strs = s.Split('+');
        Debug.Assert(strs.Length == 2);
        var guid = Guid.Parse(strs[0]);
        return new GrainID(guid, strs[1]);
    }
}