using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Data.Common;

namespace MenthaAssembly.Data.Primitives
{
    public abstract class MSSQLBase
    {
        protected void Execute(string ConnectionString, string TransactSQL, params SqlParameter[] Parameters)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);
            try
            {
                Connection.Open();
                Command.ExecuteNonQuery();
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
        protected T Execute<T>(string ConnectionString, string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);
            try
            {
                Connection.Open();
                using SqlDataReader Reader = Command.ExecuteReader();
                return Handler.Invoke(Reader);
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        protected void Execute(string ConnectionString, params SqlCommand[] Commands)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            try
            {
                Connection.Open();
                foreach (SqlCommand Command in Commands)
                {
                    Command.Connection = Connection;
                    Command.ExecuteNonQuery();
                    Command.Dispose();
                }
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
        protected IEnumerable<T> Execute<T>(string ConnectionString, Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            try
            {
                Connection.Open();

                foreach (SqlCommand Command in Commands)
                {
                    Command.Connection = Connection;
                    using SqlDataReader Reader = Command.ExecuteReader();
                    yield return Handler.Invoke(Reader);
                    Command.Dispose();
                }
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        protected async Task ExecuteAsync(string ConnectionString, string TransactSQL, params SqlParameter[] Parameters)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);

            try
            {
                await Connection.OpenAsync();
                await Command.ExecuteNonQueryAsync();
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
        protected async Task<T> ExecuteAsync<T>(string ConnectionString, string TransactSQL, Func<SqlDataReader, T> Handler, params SqlParameter[] Parameters)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);
            try
            {
                await Connection.OpenAsync();
                using SqlDataReader Reader = await Command.ExecuteReaderAsync();
                return await Task.Run(() => Handler.Invoke(Reader));
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        protected async Task ExecuteAsync(string ConnectionString, params SqlCommand[] Commands)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            try
            {
                await Connection.OpenAsync();
                foreach (SqlCommand Command in Commands)
                {
                    Command.Connection = Connection;
                    await Command.ExecuteNonQueryAsync();
                    Command.Dispose();
                }
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
        protected async IAsyncEnumerable<T> ExecuteAsync<T>(string ConnectionString, Func<SqlDataReader, T> Handler, params SqlCommand[] Commands)
        {
            SqlConnection Connection = new SqlConnection(ConnectionString);
            try
            {
                await Connection.OpenAsync();

                foreach (SqlCommand Command in Commands)
                {
                    Command.Connection = Connection;
                    using SqlDataReader Reader = await Command.ExecuteReaderAsync();
                    yield return Handler.Invoke(Reader);
                    Command.Dispose();
                }
            }
            finally
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

    }
}
