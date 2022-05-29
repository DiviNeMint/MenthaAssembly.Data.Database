using MenthaAssembly.Data.Primitives;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace MenthaAssembly.Data
{
    public class MSSQL : MSSQLBase
    {
        internal SqlConnectionStringBuilder Builder { get; } = new SqlConnectionStringBuilder
        {
            IntegratedSecurity = true,
            ConnectTimeout = 3
        };

        private const string LocalAddress = "localhost";
        private string _Address = "localhost";
        public string Address
        {
            get => _Address;
            set
            {
                _Address = string.IsNullOrEmpty(value) ? LocalAddress : value;
                Builder.DataSource = LocalAddress.Equals(_Address) ?
                                     IsExpress ? $"{LocalAddress}\\SQLEXPRESS" : LocalAddress :
                                     $"{_Address},{_Port}";
            }
        }

        private int _Port = 1433;
        public int Port
        {
            get => _Port;
            set
            {
                _Port = value;
                Builder.DataSource = LocalAddress.Equals(_Address) ?
                                     IsExpress ? $"{LocalAddress}\\SQLEXPRESS" : LocalAddress :
                                     $"{_Address},{_Port}";
            }
        }

        public string UserID
        {
            get => Builder.UserID;
            set
            {
                Builder.UserID = value;
                Builder.IntegratedSecurity = string.IsNullOrEmpty(value);
            }
        }

        public string Password
        {
            get => Builder.Password;
            set => Builder.Password = value;
        }

        private bool _IsExpress;
        public bool IsExpress
        {
            get => _IsExpress;
            set
            {
                _IsExpress = value;
                if (value &&
                    (string.IsNullOrEmpty(_Address) || LocalAddress.Equals(_Address)))
                    Builder.DataSource = $"{LocalAddress}\\SQLEXPRESS";
            }
        }
MenthaAssembly.Data.Database
        public int Timeout
        {
            get => Builder.ConnectTimeout;
            set => Builder.ConnectTimeout = value;
        }

        public MSSQL() : this(null)
        {
        }
        public MSSQL(string IPAddress)
        {
            Address = string.IsNullOrEmpty(IPAddress) ? LocalAddress : IPAddress;
        }
        public MSSQL(string IPAddress, string UserID, string Password) : this(IPAddress)
        {
            this.UserID = UserID;
            this.Password = Password;
        }
        public MSSQL(string IPAddress, int Port) : this(IPAddress)
        {
            this.Port = Port;
        }
        public MSSQL(string IPAddress, int Port, string UserID, string Password) : this(IPAddress, Port)
        {
            this.UserID = UserID;
            this.Password = Password;
        }

        public MSSQLDatabase this[string DatabaseName] => new MSSQLDatabase(this, DatabaseName);

        public async Task<bool> ConnectionTest()
        {
            SqlConnection Connection = new SqlConnection(Builder.ConnectionString);
            try
            {
                await Connection.OpenAsync();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message}");
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
            return false;
        }

        public async Task<string[]> GetDatabaseNames()
            => await ExecuteAsync(
                "SELECT name FROM sys.Databases",
                (Reader) =>
                {
                    List<string> Result = new List<string>();
                    while (Reader.Read())
                        Result.Add(Reader["name"].ToString());

                    return Result.ToArray();
                });

        public async Task<bool> Contain(string DatabaseName)
            => await ExecuteAsync("Select name From sys.Databases Where name = @Name",
                             (Reader) => Reader.Read(),
                             new SqlParameter("@Name", DatabaseName));

        public async Task<bool> Create(string DatabaseName)
        {
            try
            {
                //string sql = $"Create Database {DatabaseName} " +
                //             $"On Primary (Name={DatabaseName}, Filename='{Path.Combine(FilePath, $"{DatabaseName}.mdf")}', Size=5, Maxsize=UNLIMITED, Filegrowth=10%) " +
                //             $"Log On (Name={DatabaseName}_log, Filename='{Path.Combine(FilePath, $"{DatabaseName}_log.ldf")}', Size=1, Maxsize=20, Filegrowth=1)";

                await ExecuteAsync(Builder.ConnectionString, $"Create Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        public async Task<bool> Delete(string DatabaseName)
        {
            try
            {
                await ExecuteAsync(Builder.ConnectionString, $"Drop Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public async Task<bool> Delete(MSSQLDatabase Database)
            => await Delete(Database.Name);

        public void Execute(string TransactSQL, params SqlParameter[] Parameters)
             => Execute(Builder.ConnectionString, TransactSQL, Parameters);
        public T Execute<T>(string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
            => Execute(Builder.ConnectionString, TransactSQL, Handler, Parameters);

        public void Execute(params SqlCommand[] Commands)
            => Execute(Builder.ConnectionString, Commands);
        public IEnumerable<T> Execute<T>(Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
            => Execute(Builder.ConnectionString, Handler, Commands);

        public async Task ExecuteAsync(string TransactSQL, params SqlParameter[] Parameters)
             => await ExecuteAsync(Builder.ConnectionString, TransactSQL, Parameters);
        public async Task<T> ExecuteAsync<T>(string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
            => await ExecuteAsync(Builder.ConnectionString, TransactSQL, Handler, Parameters);

        public async Task ExecuteAsync(params SqlCommand[] Commands)
            => await ExecuteAsync(Builder.ConnectionString, Commands);
        public async IAsyncEnumerable<T> ExecuteAsync<T>(Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
        {
            await foreach (T Item in ExecuteAsync(Builder.ConnectionString, Handler, Commands))
                yield return Item;
        }

    }

}
