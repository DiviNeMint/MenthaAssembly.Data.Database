using MenthaAssembly.Data.Primitives;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace MenthaAssembly.Data
{
    public class MSSQLDatabase : MSSQLBase
    {
        internal MSSQL Parent { get; }

        public string Name { get; }

        public MSSQLTable this[string TableName]
            => new MSSQLTable(this, TableName);

        internal MSSQLDatabase(MSSQL MSSQL, string Name)
        {
            Parent = MSSQL;
            this.Name = Name;
        }

        public async Task<bool> Create<T>(string TableName)
            where T : class
        {
            try
            {
                IEnumerable<string> Properties = typeof(T).GetProperties()
                                                          .Select(i =>
                                                                  {
                                                                      if (i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute)
                                                                          return $"{i.Name} {Attribute.ToString()}";
                                                                      return string.Empty;
                                                                  })
                                                           .Where(i => !string.IsNullOrEmpty(i));
                await ExecuteAsync($"Create Table {TableName} ({string.Join(", ", Properties)})");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        public async Task<bool> Delete(string TableName)
        {
            try
            {
                await ExecuteAsync($"Drop Table {TableName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public async Task<bool> Delete(MSSQLTable Table)
            => await Delete(Table.Name);

        public async Task<string[]> GetTableNames()
            => await ExecuteAsync(
                "Select name From sys.Tables",
                (Reader) =>
                {
                    List<string> Result = new List<string>();
                    while (Reader.Read())
                        Result.Add(Reader["name"].ToString());

                    return Result.ToArray();
                });

        public async Task<bool> Contain(string TableName)
            => await ExecuteAsync("Select name From sys.Tables Where name = @Name",
                             (Reader) => Reader.Read(),
                             new SqlParameter("@Name", TableName));

        public void Execute(string TransactSQL, params SqlParameter[] Parameters)
        {
            Parent.Builder.InitialCatalog = Name;
            Execute(Parent.Builder.ConnectionString, TransactSQL, Parameters);
        }
        public T Execute<T>(string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
        {
            Parent.Builder.InitialCatalog = Name;
            return Execute(Parent.Builder.ConnectionString, TransactSQL, Handler, Parameters);
        }

        public void Execute(params SqlCommand[] Commands)
        {
            Parent.Builder.InitialCatalog = Name;
            Execute(Parent.Builder.ConnectionString, Commands);
        }
        public IEnumerable<T> Execute<T>(Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
        {
            Parent.Builder.InitialCatalog = Name;
            return Execute(Parent.Builder.ConnectionString, Handler, Commands);
        }

        public async Task ExecuteAsync(string TransactSQL, params SqlParameter[] Parameters)
        {
            Parent.Builder.InitialCatalog = Name;
            await ExecuteAsync(Parent.Builder.ConnectionString, TransactSQL, Parameters);
        }
        public async Task<T> ExecuteAsync<T>(string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
        {
            Parent.Builder.InitialCatalog = Name;
            return await ExecuteAsync(Parent.Builder.ConnectionString, TransactSQL, Handler, Parameters);
        }

        public async Task ExecuteAsync(params SqlCommand[] Commands)
        {
            Parent.Builder.InitialCatalog = Name;
            await ExecuteAsync(Parent.Builder.ConnectionString, Commands);
        }
        public async IAsyncEnumerable<T> ExecuteAsync<T>(Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
        {
            Parent.Builder.InitialCatalog = Name;

            await foreach (T Item in ExecuteAsync(Parent.Builder.ConnectionString, Handler, Commands))
                yield return Item;
        }

        internal protected string GetConnectionString()
        {
            Parent.Builder.InitialCatalog = Name;
            return Parent.Builder.ConnectionString;
        }

    }
}
