using MenthaAssembly.Data.Primitives;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace MenthaAssembly.Data
{
    public class MSSQLTable : MSSQLBase
    {
        internal MSSQLDatabase Database { get; }

        public string Name { get; }

        internal MSSQLTable(MSSQLDatabase Database, string Name)
        {
            this.Database = Database;
            this.Name = Name;
        }

        public async Task<bool> Add<T>(T Item)
            where T : class, new()
        {
            List<string> Properties = new List<string>();
            List<SqlParameter> Parameters = new List<SqlParameter>();
            List<PropertyInfo> OutputProperties = new List<PropertyInfo>();

            foreach (PropertyInfo Info in Item.GetType().GetProperties())
            {
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                {
                    if (Attribute.IsIdentity)
                    {
                        OutputProperties.Add(Info);
                        continue;
                    }

                    object Value = Info.GetValue(Item);
                    if (Value is null &&
                        (Attribute.IsPrimaryKey || !Attribute.Nullable))
                        throw new ArgumentNullException($"Property {Info.Name} can't be null.");

                    Properties.Add(Info.Name);
                    Parameters.Add(new SqlParameter($"@{Info.Name}", Value ?? DBNull.Value));
                }
            }

            return await Database.ExecuteAsync($"Insert Into [{Name}] " +
                                          $"([{string.Join("], [", Properties)}]) " +
                                          (OutputProperties.Count > 0 ? $"Output {string.Join(", ", OutputProperties.Select(i => $"Inserted.{i.Name}"))} " : "") +
                                          $"Values({string.Join(", ", Parameters.Select(i => i.ParameterName))})",
                                          (Reader) =>
                                          {
                                              if (OutputProperties.Count > 0)
                                              {
                                                  if (!Reader.Read())
                                                      return false;

                                                  foreach (PropertyInfo Info in OutputProperties)
                                                  {
                                                      object Value = Reader[Info.Name];
                                                      if (Value is DBNull)
                                                      {
                                                          Info.SetValue(Item, null);
                                                          continue;
                                                      }

                                                      Info.SetValue(Item, Convert.ChangeType(Value.ToString(), Info.PropertyType));
                                                  }
                                              }
                                              return true;
                                          },
                                          Parameters.ToArray());
        }
        public async IAsyncEnumerable<bool> Add<T>(IEnumerable<T> Items)
            where T : class
        {
            List<PropertyInfo> OutputProperties = new List<PropertyInfo>();
            Dictionary<PropertyInfo, MSSQLAttribute> PropertyInfos = new Dictionary<PropertyInfo, MSSQLAttribute>();
            foreach (PropertyInfo Info in typeof(T).GetProperties())
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                    if (Attribute.IsIdentity)
                        OutputProperties.Add(Info);
                    else
                        PropertyInfos.Add(Info, Attribute);

            string TransactSQL = $"Insert Into [{Name}] " +
                                 $"([{string.Join("], [", PropertyInfos.Keys.Select(i => i.Name))}]) " +
                                 (OutputProperties.Count > 0 ? $"Output {string.Join(", ", OutputProperties.Select(i => $"Inserted.{i.Name}"))} " : "") +
                                 $"Values({string.Join(", ", PropertyInfos.Keys.Select(i => $"@{i.Name}"))})";

            if (OutputProperties.Count == 0)
                yield break;

            IEnumerator<T> Enumerator = Items.GetEnumerator();
            if (Enumerator.MoveNext())
                yield break;

            SqlCommand[] Commands = Items.Select(i =>
            {
                SqlCommand Command = new SqlCommand(TransactSQL);
                foreach (KeyValuePair<PropertyInfo, MSSQLAttribute> Info in PropertyInfos)
                {
                    object Value = Info.Key.GetValue(i);
                    if (Value is null &&
                        (Info.Value.IsPrimaryKey || !Info.Value.Nullable))
                        throw new ArgumentNullException($"Property {Info.Key.Name} can't be null.");

                    Command.Parameters.AddWithValue($"@{Info.Key.Name}", Value ?? DBNull.Value);
                }
                return Command;
            }).ToArray();

            bool Handle(SqlDataReader Reader)
            {
                if (!Reader.Read())
                    return false;

                foreach (PropertyInfo Info in OutputProperties)
                {
                    object Value = Reader[Info.Name];
                    if (Value is DBNull)
                    {
                        Info.SetValue(Enumerator.Current, null);
                        continue;
                    }

                    Info.SetValue(Enumerator.Current, Convert.ChangeType(Value.ToString(), Info.PropertyType));
                }

                return true;
            }

            await foreach (bool Success in Database.ExecuteAsync(Handle, Commands))
                yield return Success;
        }

        public async Task Remove<T>(T Item)
            where T : class, new()
        {
            PropertyInfo[] Properties = Item.GetType().GetProperties();
            if (Properties.FirstOrDefault(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute &&
                                               Attribute.IsPrimaryKey) is PropertyInfo PrimaryProperty)
            {
                await Database.ExecuteAsync($"Delete From [{Name}] Where {PrimaryProperty.Name}=@{PrimaryProperty.Name}",
                                       new SqlParameter($"@{PrimaryProperty.Name}", PrimaryProperty.GetValue(Item)));
                return;
            }

            List<string> PropertyNames = new List<string>();
            List<SqlParameter> Parameters = new List<SqlParameter>();

            foreach (PropertyInfo Info in Properties)
            {
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                {
                    object Value = Info.GetValue(Item);
                    if (Value is null &&
                        (Attribute.IsPrimaryKey || !Attribute.Nullable))
                        throw new ArgumentNullException($"Property {Info.Name} can't be null.");

                    PropertyNames.Add(Info.Name);
                    Parameters.Add(new SqlParameter($"@{Info.Name}", Value ?? DBNull.Value));
                }
            }

            await Database.ExecuteAsync($"Delete From [{Name}] Where {string.Join(" And ", PropertyNames.Select(i => $"{i}=@{i}"))}",
                                   Parameters.ToArray());
        }
        public async Task Remove<T>(IEnumerable<T> Items)
            where T : class, new()
        {
            string TransactSQL = string.Empty;
            PropertyInfo[] Properties = typeof(T).GetProperties();
            if (Properties.FirstOrDefault(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute &&
                                               Attribute.IsPrimaryKey) is PropertyInfo PrimaryProperty)
            {
                TransactSQL = $"Delete From [{Name}] Where {PrimaryProperty.Name}=@{PrimaryProperty.Name}";

                await Database.ExecuteAsync(Items.Select(i =>
                {
                    SqlCommand Cmd = new SqlCommand(TransactSQL);
                    Cmd.Parameters.AddWithValue($"@{PrimaryProperty.Name}", PrimaryProperty.GetValue(i));
                    return Cmd;
                }).ToArray());
                return;
            }

            Dictionary<PropertyInfo, MSSQLAttribute> PropertyInfos = new Dictionary<PropertyInfo, MSSQLAttribute>();
            foreach (PropertyInfo Info in typeof(T).GetProperties())
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                    PropertyInfos.Add(Info, Attribute);

            TransactSQL = $"Delete From [{Name}] Where {string.Join(" And ", PropertyInfos.Keys.Select(i => $"{i.Name}=@{i.Name}"))}";
            await Database.ExecuteAsync(
                Items.Select(i =>
                {
                    SqlCommand Cmd = new SqlCommand(TransactSQL);
                    foreach (KeyValuePair<PropertyInfo, MSSQLAttribute> Info in PropertyInfos)
                    {
                        object Value = Info.Key.GetValue(i);
                        if (Value is null &&
                            (Info.Value.IsPrimaryKey || !Info.Value.Nullable))
                            throw new ArgumentNullException($"Property {Info.Key.Name} can't be null.");

                        Cmd.Parameters.AddWithValue($"@{Info.Key.Name}", Value ?? DBNull.Value);
                    }
                    return Cmd;
                }).ToArray());
        }
        public async Task Remove<T>(string Condition, params SqlParameter[] Parameters)
            where T : class, new()
        {
            if (Condition.Length is 0)
                return;

            await Database.ExecuteAsync($"Delete From [{Name}] Where {Condition}", Parameters);
        }
        public async Task Remove<T>(Expression<Func<T, bool>> Prediction)
            where T : new()
            => await Where(Prediction).Remove();

        public async Task Modify<T>(T Item, params string[] PropertyNames)
            where T : class, new()
        {
            if (!(Item.GetType().GetProperties()
                                .FirstOrDefault(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute &&
                                                     Attribute.IsPrimaryKey) is PropertyInfo PrimaryProperty))
                throw new ArgumentNullException("Can't find PrimaryKey's Property.");

            await Modify(Item,
                         PropertyNames,
                         $"{PrimaryProperty.Name}=@{PrimaryProperty.Name}",
                         new SqlParameter($"@{PrimaryProperty.Name}", PrimaryProperty.GetValue(Item)));
        }
        public async Task Modify<T>(T Item, string[] PropertyNames, string Condition, params SqlParameter[] CoditionParameter)
            where T : class, new()
        {
            List<string> ModifyCommands = new List<string>();
            List<SqlParameter> Parameters = new List<SqlParameter>(CoditionParameter);

            foreach (PropertyInfo Info in Item.GetType().GetProperties(PropertyNames))
            {
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                {
                    if (Attribute.IsPrimaryKey)
                        throw new NotImplementedException("Can't modify PrimaryKey's value.");

                    object Value = Info.GetValue(Item);
                    if (Value is null &&
                        !Attribute.Nullable)
                        throw new ArgumentNullException($"Property {Info.Name} can't be null.");

                    string ArgumentName = $"@Arg{Parameters.Count}";
                    ModifyCommands.Add($"{Info.Name}={ArgumentName}");
                    Parameters.Add(new SqlParameter(ArgumentName, Value ?? DBNull.Value));
                }
            }

            await Database.ExecuteAsync($"Update [{Name}] " +
                                   $"Set {string.Join(", ", ModifyCommands)} " +
                                   $"Where {Condition}",
                                   Parameters.ToArray());
        }
        public async Task Modify<T>(IEnumerable<T> Items, params string[] PropertyNames)
            where T : class, new()
        {
            if (!(typeof(T).GetProperties()
                           .FirstOrDefault(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute &&
                                                Attribute.IsPrimaryKey) is PropertyInfo PrimaryProperty))
                throw new ArgumentNullException("Can't find PrimaryKey's Property.");

            await Modify(Items,
                         PropertyNames,
                         $"{PrimaryProperty.Name}=@{PrimaryProperty.Name}",
                         i => new SqlParameter($"@{PrimaryProperty.Name}", PrimaryProperty.GetValue(i)));
        }
        public async Task Modify<T>(IEnumerable<T> Items, string[] PropertyNames, string Condition, params Func<T, SqlParameter>[] CoditionParameterFunc)
            where T : class, new()
        {
            List<string> ModifyCommands = new List<string>();
            Dictionary<PropertyInfo, MSSQLAttribute> PropertyInfos = new Dictionary<PropertyInfo, MSSQLAttribute>();
            foreach (PropertyInfo Info in typeof(T).GetProperties(PropertyNames))
                if (Info.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                {
                    if (Attribute.IsPrimaryKey)
                        throw new NotImplementedException("Can't modify PrimaryKey's value.");

                    string ArgumentName = $"@Arg{ModifyCommands.Count}";
                    ModifyCommands.Add($"{Info.Name}={ArgumentName}");
                    PropertyInfos.Add(Info, Attribute);
                }

            string TransactSQL = $"Update [{Name}] " +
                                 $"Set {string.Join(", ", ModifyCommands)} " +
                                 $"Where {Condition}";

            await Database.ExecuteAsync(
                Items.Select(i =>
                {
                    SqlCommand Cmd = new SqlCommand(TransactSQL);
                    // Parameter
                    foreach (KeyValuePair<PropertyInfo, MSSQLAttribute> Info in PropertyInfos)
                    {
                        object Value = Info.Key.GetValue(i);
                        if (Value is null &&
                            !Info.Value.Nullable)
                            throw new ArgumentNullException($"Property {Info.Key.Name} can't be null.");

                        Cmd.Parameters.AddWithValue($"@Arg{Cmd.Parameters.Count}", Value ?? DBNull.Value);
                    }

                    // Condition Parameter
                    foreach (Func<T, SqlParameter> Func in CoditionParameterFunc)
                        Cmd.Parameters.Add(Func.Invoke(i));

                    return Cmd;
                }).ToArray());
        }

        public IAsyncEnumerable<T> As<T>()
            where T : new()
            => new MSSQLLinq<T>(this);

        public IAsyncEnumerable<U> Select<T, U>(Expression<Func<T, U>> Selector)
            where T : new()
            => new MSSQLLinq<T>(this).Select(Selector);

        public IDatabaseSelectLinq<T> Select<T>(Expression<Func<T, object>> Selector)
            where T : new()
            => new MSSQLLinq<T>(this).Select(Selector);

        public IDatabaseLinq<T> Where<T>(Expression<Func<T, bool>> Prediction)
            where T : new()
            => new MSSQLLinq<T>(this).Where(Prediction);

    }

}
