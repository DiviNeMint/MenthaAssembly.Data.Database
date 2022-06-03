using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace MenthaAssembly.Data.Primitives
{
    public abstract class MSSQLBase
    {
        internal protected abstract SqlConnection CreateConnection();

        public void Execute(string TransactSQL, params SqlParameter[] Parameters)
        {
            using SqlConnection Connection = CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);

            Connection.Open();
            Command.ExecuteNonQuery();
        }
        public void Execute(params SqlCommand[] Commands)
        {
            using SqlConnection Connection = CreateConnection();

            Connection.Open();
            foreach (SqlCommand Command in Commands)
            {
                Command.Connection = Connection;
                Command.ExecuteNonQuery();
                Command.Dispose();
            }
        }

        public async Task ExecuteAsync(string TransactSQL, params SqlParameter[] Parameters)
        {
            using SqlConnection Connection = CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);

            await Connection.OpenAsync();
            await Command.ExecuteNonQueryAsync();
        }
        public async Task ExecuteAsync(params SqlCommand[] Commands)
        {
            using SqlConnection Connection = CreateConnection();

            await Connection.OpenAsync();
            foreach (SqlCommand Command in Commands)
            {
                Command.Connection = Connection;
                await Command.ExecuteNonQueryAsync();
                Command.Dispose();
            }
        }

        internal protected SqlDataReader ExcuteReader(string TransactSQL, params SqlParameter[] Parameters)
        {
            using SqlConnection Connection = CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);

            Connection.Open();
            return Command.ExecuteReader();
        }
        internal protected IEnumerable<SqlDataReader> ExcuteReader(params SqlCommand[] Commands)
        {
            using SqlConnection Connection = CreateConnection();
            Connection.Open();

            foreach (SqlCommand Command in Commands)
            {
                Command.Connection = Connection;
                yield return Command.ExecuteReader();
                Command.Dispose();
            }
        }

        internal protected async Task<SqlDataReader> ExcuteReaderAsync(string TransactSQL, params SqlParameter[] Parameters)
        {
            using SqlConnection Connection = CreateConnection();
            using SqlCommand Command = new SqlCommand(TransactSQL, Connection);
            Command.Parameters.AddRange(Parameters);

            await Connection.OpenAsync();
            return await Command.ExecuteReaderAsync();
        }
        internal protected async IAsyncEnumerable<SqlDataReader> ExcuteReaderAsync(params SqlCommand[] Commands)
        {
            using SqlConnection Connection = CreateConnection();
            await Connection.OpenAsync();

            foreach (SqlCommand Command in Commands)
            {
                Command.Connection = Connection;
                yield return await Command.ExecuteReaderAsync();
                Command.Dispose();
            }
        }

    }
}
