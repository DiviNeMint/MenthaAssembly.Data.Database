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

        public bool Create<T>(string TableName)
            where T : class
        {
            try
            {
                IEnumerable<string> Properties = typeof(T).GetProperties()
                                                          .Select(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute ? $"{i.Name} {Attribute}" : null)
                                                          .Where(i => i != null);
                Execute($"Create Table {TableName} ({string.Join(", ", Properties)})");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public async Task<bool> CreateAsync<T>(string TableName)
            where T : class
        {
            try
            {
                IEnumerable<string> Properties = typeof(T).GetProperties()
                                                          .Select(i => i.GetCustomAttribute<MSSQLAttribute>() is MSSQLAttribute Attribute ? $"{i.Name} {Attribute}" : null)
                                                          .Where(i => i != null);
                await ExecuteAsync($"Create Table {TableName} ({string.Join(", ", Properties)})");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        public bool Delete(string TableName)
        {
            try
            {
                Execute($"Drop Table {TableName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public bool Delete(MSSQLTable Table)
            => Delete(Table.Name);
        public async Task<bool> DeleteAsync(string TableName)
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
        public async Task<bool> DeleteAsync(MSSQLTable Table)
            => await DeleteAsync(Table.Name);

        public IEnumerable<string> GetTableNames()
        {
            using SqlDataReader Reader = ExcuteReader("Select name From sys.Tables");
            while (Reader.Read())
                yield return Reader["name"].ToString();
        }
        public async IAsyncEnumerable<string> GetTableNamesAsync()
        {
            using SqlDataReader Reader = await ExcuteReaderAsync("Select name From sys.Tables");
            while (Reader.Read())
                yield return Reader["name"].ToString();
        }

        public bool Contain(string TableName)
        {
            using SqlDataReader Reader = ExcuteReader("Select name From sys.Tables Where name = @Name", new SqlParameter("@Name", TableName));
            return Reader.Read();
        }
        public async Task<bool> ContainAsync(string TableName)
        {
            using SqlDataReader Reader = await ExcuteReaderAsync("Select name From sys.Tables Where name = @Name", new SqlParameter("@Name", TableName));
            return Reader.Read();
        }

        internal protected override SqlConnection CreateConnection()
        {
            Parent.Builder.InitialCatalog = Name;
            return new SqlConnection(Parent.Builder.ConnectionString);
        }

    }
}
