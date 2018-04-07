using DbBenchmark.Mocks;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DbBenchmark
{
    public class PostgreDb : Db
    {
        private NpgsqlConnection _connection;
        private string _tablename;

        public PostgreDb(string connectionString, string tableName)
        {
            _connection = new NpgsqlConnection(connectionString);
            _tablename = tableName;
        }

        public async Task ResetTable()
        {
            try
            {
                await _connection.OpenAsync();
                var cmdDropTable = new NpgsqlCommand($"DROP TABLE IF EXISTS {_tablename}", _connection);
                var resultDropTable = cmdDropTable.ExecuteScalar();
                var cmdCreateTable = new NpgsqlCommand($"CREATE TABLE {_tablename} (id serial NOT NULL,column1 varchar,column2 varchar,column3 varchar,column4 varchar,column5 varchar,column6 varchar,column7 varchar,column8 varchar,column9 varchar,CONSTRAINT pk_table PRIMARY KEY(id))", _connection);
                var resultCreateTable = cmdCreateTable.ExecuteScalar();
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
                    NpgsqlCommand cmd = _connection.CreateCommand();
                    cmd.CommandText = $"INSERT INTO {_tablename} (id,column1,column2,column3,column4,column5,column6,column7,column8,column9) VALUES " +
                        $"(:id,:column1,:column2,:column3,:column4,:column5,:column6,:column7,:column8,:column9)";
                    cmd.Parameters.Add(new NpgsqlParameter(":id", item.Id));
                    cmd.Parameters.Add(new NpgsqlParameter(":column1", item.Column1));
                    cmd.Parameters.Add(new NpgsqlParameter(":column2", item.Column2));
                    cmd.Parameters.Add(new NpgsqlParameter(":column3", item.Column3));
                    cmd.Parameters.Add(new NpgsqlParameter(":column4", item.Column4));
                    cmd.Parameters.Add(new NpgsqlParameter(":column5", item.Column5));
                    cmd.Parameters.Add(new NpgsqlParameter(":column6", item.Column6));
                    cmd.Parameters.Add(new NpgsqlParameter(":column7", item.Column7));
                    cmd.Parameters.Add(new NpgsqlParameter(":column8", item.Column8));
                    cmd.Parameters.Add(new NpgsqlParameter(":column9", item.Column9));
                    cmd.ExecuteNonQuery();
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
                    var cmd = new NpgsqlCommand($"SELECT * FROM {_tablename} WHERE id = {item.Id}", _connection);
                    cmd.ExecuteScalar();
                }
                _connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
            }
        }

        public async Task WriteBulk(List<Item> items)
        {
            try
            {
                await _connection.OpenAsync();
                var writer = _connection.BeginBinaryImport($"COPY {_tablename} (id,column1,column2,column3,column4,column5,column6,column7,column8,column9) FROM STDIN (FORMAT BINARY)");
                using (writer)
                {
                    foreach (var item in items)
                    {
                        writer.WriteRow(
                            item.Id,
                            item.Column1,
                            item.Column2,
                            item.Column3,
                            item.Column4,
                            item.Column5,
                            item.Column6,
                            item.Column7,
                            item.Column8,
                            item.Column9
                        );
                    }
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
