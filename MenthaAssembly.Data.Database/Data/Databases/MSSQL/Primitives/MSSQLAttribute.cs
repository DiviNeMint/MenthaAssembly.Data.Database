using System;
using System.Data;

namespace MenthaAssembly.Data
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public sealed class MSSQLAttribute : Attribute
    {
        public SqlDbType ValueType { get; }

        public int Size { set; get; } = -1;

        public bool Nullable { set; get; }

        public bool IsPrimaryKey { set; get; }

        public bool IsIdentity { set; get; }

        public int IdentitySeed { set; get; } = 1;
        public int IdentityDelta { set; get; } = 1;

        public MSSQLAttribute(SqlDbType ValueType)
        {
            this.ValueType = ValueType;
        }

        public override string ToString()
        {
            string Result = ValueType.ToString();
            if (Size > 0)
                Result += $"({(int.MaxValue.Equals(Size) ? "Max" : Size.ToString())})";

            if (IsPrimaryKey)
                Result += " Primary Key";

            if (IsIdentity)
                Result += $" Identity({IdentitySeed},{IdentityDelta})";

            if (!IsPrimaryKey &&!Nullable)
                Result += " Not Null";

            return Result;
        }

    }

}
