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

        public bool ConnectionTest()
        {
            try
            {
                using SqlConnection Connection = CreateConnection();
                Connection.Open();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message}");
            }

            return false;
        }
        public async Task<bool> ConnectionTestAsync()
        {
            try
            {
                using SqlConnection Connection = CreateConnection();
                await Connection.OpenAsync();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message}");
            }

            return false;
        }

        public IEnumerable<string> GetDatabaseNames()
        {
            using SqlDataReader Reader = ExcuteReader("SELECT name FROM sys.Databases");
            while (Reader.Read())
                yield return Reader["name"].ToString();
        }
        public async IAsyncEnumerable<string> GetDatabaseNamesAsync()
        {
            using SqlDataReader Reader = await ExcuteReaderAsync("SELECT name FROM sys.Databases");
            while (Reader.Read())
                yield return Reader["name"].ToString();
        }

        public bool Contain(string DatabaseName)
        {
            using SqlDataReader Reader = ExcuteReader("Select name From sys.Databases Where name = @Name", new SqlParameter("@Name", DatabaseName));
            return Reader.Read();
        }
        public async Task<bool> ContainAsync(string DatabaseName)
        {
            using SqlDataReader Reader = await ExcuteReaderAsync("Select name From sys.Databases Where name = @Name", new SqlParameter("@Name", DatabaseName));
            return Reader.Read();
        }

        public bool Create(string DatabaseName)
        {
            try
            {
                //string sql = $"Create Database {DatabaseName} " +
                //             $"On Primary (Name={DatabaseName}, Filename='{Path.Combine(FilePath, $"{DatabaseName}.mdf")}', Size=5, Maxsize=UNLIMITED, Filegrowth=10%) " +
                //             $"Log On (Name={DatabaseName}_log, Filename='{Path.Combine(FilePath, $"{DatabaseName}_log.ldf")}', Size=1, Maxsize=20, Filegrowth=1)";

                Execute($"Create Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public async Task<bool> CreateAsync(string DatabaseName)
        {
            try
            {
                //string sql = $"Create Database {DatabaseName} " +
                //             $"On Primary (Name={DatabaseName}, Filename='{Path.Combine(FilePath, $"{DatabaseName}.mdf")}', Size=5, Maxsize=UNLIMITED, Filegrowth=10%) " +
                //             $"Log On (Name={DatabaseName}_log, Filename='{Path.Combine(FilePath, $"{DatabaseName}_log.ldf")}', Size=1, Maxsize=20, Filegrowth=1)";

                await ExecuteAsync($"Create Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        public bool Delete(string DatabaseName)
        {
            try
            {
                Execute($"Drop Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public bool Delete(MSSQLDatabase Database)
            => Delete(Database.Name);
        public async Task<bool> DeleteAsync(string DatabaseName)
        {
            try
            {
                await ExecuteAsync($"Drop Database {DatabaseName}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        public async Task<bool> DeleteAsync(MSSQLDatabase Database)
            => await DeleteAsync(Database.Name);

        protected internal override SqlConnection CreateConnection()
        {
            Builder.InitialCatalog = string.Empty;
            return new SqlConnection(Builder.ConnectionString);
        }

    }

}
