using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace MenthaAssembly.Data.Primitives
{
    internal class MSSQLLinq<T> : IDatabaseLinq<T>
    {
        internal readonly MSSQLDatabase Database;
        internal readonly string FromExpression = string.Empty;
        internal string SelectExpression = string.Empty;
        internal string WhereExpression = string.Empty;
        internal List<SqlParameter> Parameters = new List<SqlParameter>();

        internal MSSQLLinq(MSSQLDatabase Database, string From)
        {
            this.Database = Database;
            FromExpression = From;
        }

        public IDatabaseLinq<U> Select<U>(Expression<Func<T, U>> Selector)
        {
            string PropertyNames = string.Join(", ", MSSQLExpression.Parse(Selector));
            SelectExpression = string.IsNullOrEmpty(SelectExpression) ?
                               PropertyNames :
                               $"{SelectExpression}, {PropertyNames}";

            return new MSSQLLinq<U>(Database, FromExpression)
            {
                SelectExpression = SelectExpression,
                WhereExpression = WhereExpression,
                Parameters = Parameters
            };
        }
        public IDatabaseLinq<T> Select(params Expression<Func<T, object>>[] Selectors)
        {
            string PropertyNames = string.Join(", ", Selectors.Select(i => MSSQLExpression.Parse(i)));
            SelectExpression = string.IsNullOrEmpty(SelectExpression) ?
                               PropertyNames :
                               $"{SelectExpression}, {PropertyNames}";
            return this;
        }

        public IDatabaseLinq<T> Where(Expression<Func<T, bool>> Predict)
        {
            string Context = MSSQLExpression.Parse(Predict, ref Parameters);
            WhereExpression = string.IsNullOrEmpty(WhereExpression) ?
                               Context :
                               $"{WhereExpression} And {Context}";
            return this;
        }

        public async Task<IEnumerable<T>> Query()
        {
            if (string.IsNullOrEmpty(SelectExpression))
                SelectExpression = "*";

            List<T> Results = new List<T>();
            bool Success = await Database.ExecuteAsync(
                $"Select {SelectExpression} From [{FromExpression}]{(string.IsNullOrEmpty(WhereExpression) ? "" : $"Where {WhereExpression}")}",
                (Reader) =>
                {
                    Type ResultType = typeof(T);
                    bool IsBaseType = ResultType.IsValueType || typeof(string).Equals(ResultType);

                    IEnumerable<PropertyInfo> PropertyInfos = null;
                    while (Reader.Read())
                    {
                        if (PropertyInfos is null)
                        {
                            string[] DataNames = new string[Reader.FieldCount];
                            for (int i = 0; i < Reader.FieldCount; i++)
                                DataNames[i] = Reader.GetName(i);

                            PropertyInfos = typeof(T).GetProperties(DataNames);
                        }

                        T Item;
                        if (PropertyInfos.Count().Equals(0) && IsBaseType)
                        {
                            object Value = Reader[0];
                            Item = Value is DBNull ? default : (T)Value;
                        }
                        else
                        {
                            Item = (T)Activator.CreateInstance(ResultType);
                            foreach (PropertyInfo Info in PropertyInfos)
                            {
                                object Value = Reader[Info.Name];
                                if (Value is DBNull)
                                {
                                    Info.SetValue(Item, null);
                                    continue;
                                }

                                Info.SetValue(Item,
                                              typeof(Enum).Equals(Info.PropertyType.BaseType) ?
                                              Enum.Parse(Info.PropertyType, Value.ToString()) :
                                              Convert.ChangeType(Value.ToString(), Info.PropertyType));
                            }
                        }
                        Results.Add(Item);
                    }
                    return true;
                },
                Parameters.ToArray());

            if (!Success)
                return new T[0];

            return Results;
        }

        public async Task Remove()
            => await Database.ExecuteAsync($"Delete From [{FromExpression}] Where {WhereExpression}",
                                      Parameters.ToArray());

    }
}
