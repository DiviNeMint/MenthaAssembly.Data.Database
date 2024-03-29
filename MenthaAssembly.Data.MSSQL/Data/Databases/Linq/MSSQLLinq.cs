﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace MenthaAssembly.Data.Primitives
{
    internal class MSSQLLinq<T> : IDatabaseLinq<T>
    {
        internal readonly MSSQLTable Table;
        internal List<SqlParameter> Parameters = new List<SqlParameter>();

        internal MSSQLLinq(MSSQLTable Table)
        {
            this.Table = Table;
        }

        public IDatabaseSelectLinq<T> Select(Expression<Func<T, object>> Selector)
        {
            string Selected = MSSQLExpression.Parse(Selector);

            MSSQLSelectLinq<T> SelectLinq = new MSSQLSelectLinq<T>(Table)
            {
                SelectContents = new List<string> { Selected },
                WhereContents = new List<string>(WhereContents)
            };
            SelectLinq.Parameters.AddRange(Parameters);

            return SelectLinq;
        }

        public async IAsyncEnumerable<U> Select<U>(Expression<Func<T, U>> Selector)
        {
            string Selected = MSSQLExpression.Parse(Selector),
                   SelectExpression = $"Select {Selected}",
                   FromExpression = $" From [{Table.Name}]",
                   WhereContext = string.Join(" And ", WhereContents),
                   WhereExpression = string.IsNullOrEmpty(WhereContext) ? string.Empty : $" Where {WhereContext}",
                   TransactSQL = $"{SelectExpression}{FromExpression}{WhereExpression}";

            using SqlConnection Connection = Table.Database.CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters.ToArray());

            await Connection.OpenAsync();
            using SqlDataReader Reader = await Command.ExecuteReaderAsync();

            while (Reader.Read())
            {
                object Value = Reader[0];
                yield return Value is DBNull ? default : (U)Value;
            }
        }

        internal IEnumerable<string> WhereContents;
        public IDatabaseLinq<T> Where(Expression<Func<T, bool>> Prediction)
        {
            Cache = null;
            IEnumerable<string> Contexts = MSSQLExpression.Parse(Prediction, ref Parameters)
                                                          .Split(" And ");
            WhereContents = (WhereContents?.Concat(Contexts) ?? Contexts).Distinct();
            return this;
        }

        public void Remove()
        {
            string FromExpression = $"From [{Table.Name}]",
                   WhereContext = string.Join(" And ", WhereContents),
                   WhereExpression = string.IsNullOrEmpty(WhereContext) ? string.Empty : $" Where {WhereContext}";

            Table.Database.Execute($"Delete {FromExpression}{WhereExpression}", Parameters.ToArray());
        }
        public async Task RemoveAsync()
        {
            string FromExpression = $"From [{Table.Name}]",
                   WhereContext = string.Join(" And ", WhereContents),
                   WhereExpression = string.IsNullOrEmpty(WhereContext) ? string.Empty : $" Where {WhereContext}";

            await Table.Database.ExecuteAsync($"Delete {FromExpression}{WhereExpression}", Parameters.ToArray());
        }

        private async IAsyncEnumerable<T> Query()
        {
            string FromExpression = $"From [{Table.Name}]",
                   WhereContext = string.Join(" And ", WhereContents),
                   WhereExpression = string.IsNullOrEmpty(WhereContext) ? string.Empty : $" Where {WhereContext}",
                   TransactSQL = $"Select * {FromExpression}{WhereExpression}";

            using SqlConnection Connection = Table.Database.CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters.ToArray());

            await Connection.OpenAsync();
            using SqlDataReader Reader = await Command.ExecuteReaderAsync();

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

                T Item = (T)Activator.CreateInstance(typeof(T));
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

                yield return Item;
            }
        }

        private IAsyncEnumerable<T> Cache = null;
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken CancellationToken = default)
        {
            if (Cache is null)
                Cache = Query();

            return Cache.GetAsyncEnumerator(CancellationToken);
        }

    }

    internal class MSSQLSelectLinq<T> : IDatabaseSelectLinq<T>
    {
        internal readonly MSSQLTable Table;
        internal List<SqlParameter> Parameters = new List<SqlParameter>();

        internal MSSQLSelectLinq(MSSQLTable Table)
        {
            this.Table = Table;
        }

        internal IEnumerable<string> SelectContents;
        public IDatabaseSelectLinq<T> Select(Expression<Func<T, object>> Selector)
        {
            Cache = null;
            string Selected = MSSQLExpression.Parse(Selector);
            SelectContents = (SelectContents?.Append(Selected) ?? new List<string> { Selected }).Distinct();
            return this;
        }

        internal IEnumerable<string> WhereContents;
        public IDatabaseSelectLinq<T> Where(Expression<Func<T, bool>> Prediction)
        {
            Cache = null;
            IEnumerable<string> Contexts = MSSQLExpression.Parse(Prediction, ref Parameters)
                                                          .Split(" And ");
            WhereContents = (WhereContents?.Concat(Contexts) ?? Contexts).Distinct();
            return this;
        }

        private async IAsyncEnumerable<T> Query()
        {
            string SelectContext = string.Join(", ", SelectContents),
                   SelectExpression = string.IsNullOrEmpty(SelectContext) ? $"Select *" : $"Select {SelectContext}",
                   FromExpression = $" From [{Table.Name}]",
                   WhereContext = string.Join(" And ", WhereContents),
                   WhereExpression = string.IsNullOrEmpty(WhereContext) ? string.Empty : $" Where {WhereContext}",
                   TransactSQL = $"{SelectExpression}{FromExpression}{WhereExpression}";

            using SqlConnection Connection = Table.Database.CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters.ToArray());

            await Connection.OpenAsync();
            using SqlDataReader Reader = await Command.ExecuteReaderAsync();

            int SelectCount = SelectContents.Count();
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
                if (SelectCount == 1)
                {
                    object Value = Reader[0];
                    Item = Value is DBNull ? default : (T)Value;
                }
                else
                {
                    Item = (T)Activator.CreateInstance(typeof(T));
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

                yield return Item;
            }
        }

        private IAsyncEnumerable<T> Cache = null;
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken CancellationToken = default)
        {
            if (Cache is null)
                Cache = Query();

            return Cache.GetAsyncEnumerator(CancellationToken);
        }

    }

}
