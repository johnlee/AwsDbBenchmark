using DbBenchmark.Mocks;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace DbBenchmark
{
    public class SqlDb : Db
    {
        private SqlConnection _connection;
        private string _tablename;

        public SqlDb(string connectionString, string tableName)
        {
            _connection = new SqlConnection(connectionString);
            _tablename = tableName;
        }

        public async Task ResetTable()
        {
            try
            {
                await _connection.OpenAsync();
                var cmdDrop = new SqlCommand($"IF OBJECT_ID('{_tablename}', 'U') IS NOT NULL DROP TABLE {_tablename}", _connection);
                var reader = cmdDrop.ExecuteScalar();
                var cmdCreate = new SqlCommand($"CREATE TABLE {_tablename} (id nchar(10) NOT NULL,column1 varchar(255),column2 varchar(255),column3 varchar(255),column4 varchar(255),column5 varchar(255),column6 varchar(255),column7 varchar(255),column8 varchar(255),column9 varchar(255),PRIMARY KEY(id))", _connection);
                var resultCreateTable = cmdCreate.ExecuteScalar();
                _connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
            }
        }

        public async Task Write(List<Item> items)
        {
            try
            {
                await _connection.OpenAsync();
                foreach (var item in items)
                {
                    SqlCommand cmd = new SqlCommand($"INSERT INTO {_tablename} ([id],[column1],[column2],[column3],[column4],[column5],[column6],[column7],[column8],[column9]) " +
                        "VALUES (@id,@column1,@column2,@column3,@column4,@column5,@column6,@column7,@column8,@column9)", _connection);
                    cmd.Parameters.Add(new SqlParameter("@id", item.Id));
                    cmd.Parameters.Add(new SqlParameter("@column1", item.Column1));
                    cmd.Parameters.Add(new SqlParameter("@column2", item.Column2));
                    cmd.Parameters.Add(new SqlParameter("@column3", item.Column3));
                    cmd.Parameters.Add(new SqlParameter("@column4", item.Column4));
                    cmd.Parameters.Add(new SqlParameter("@column5", item.Column5));
                    cmd.Parameters.Add(new SqlParameter("@column6", item.Column6));
                    cmd.Parameters.Add(new SqlParameter("@column7", item.Column7));
                    cmd.Parameters.Add(new SqlParameter("@column8", item.Column8));
                    cmd.Parameters.Add(new SqlParameter("@column9", item.Column9));
                    await cmd.ExecuteNonQueryAsync();
                }
                _connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
            }
        }

        public async Task Read(List<Item> items)
        {
            try
            {
                await _connection.OpenAsync();
                foreach (var item in items)
                {
                    var command = new SqlCommand($"SELECT * FROM {_tablename} WHERE id = {item.Id}", _connection);
                    var reader = await command.ExecuteReaderAsync();
                    if (reader.FieldCount > 0) { }
                }
                _connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
            }
        }
    }
}
