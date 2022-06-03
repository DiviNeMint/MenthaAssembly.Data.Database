using System;
using System.Data;
using System.Text;

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
            StringBuilder Builder = new StringBuilder(ValueType.ToString());
            try
            {
                if (Size > 0)
                    Builder.Append(int.MaxValue.Equals(Size) ? "(Max)" : $"({Size})");

                if (IsPrimaryKey)
                    Builder.Append(" Primary Key");

                if (IsIdentity)
                    Builder.Append($" Identity({IdentitySeed},{IdentityDelta})");

                if (!IsPrimaryKey && !Nullable)
                    Builder.Append(" Not Null");

                return Builder.ToString();
            }
            finally
            {
                Builder.Clear();
            }
        }

    }

}
