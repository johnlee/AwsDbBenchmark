using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using DbBenchmark.Mocks;
using Microsoft.Extensions.Configuration;

namespace DbBenchmark
{
    class Program
    {
        private static IConfiguration _config;
        private static List<Item> _mockDataWrite;
        private static List<Item> _mockDataRead;

        static void Main(string[] args)
        {
            Console.WriteLine("Running Database Benchmark");
            Console.WriteLine(DateTime.Now);

            // Get AppConfig
            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            _config = configurationBuilder.Build();
            var numRowsWrite = Convert.ToInt32(_config["Global:NumberOfRecordsToWrite"].Trim());
            var numRowsRead = Convert.ToInt32(_config["Global:NumberOfRecordsToRead"].Trim());

            // Setup the mock data
            Generator mockGenerator = new Generator(numRowsWrite);
            _mockDataWrite = mockGenerator.GenerateData();
            _mockDataRead = mockGenerator.SelectRandoms(_mockDataWrite, numRowsRead);
            Console.WriteLine($"Mock data has {_mockDataWrite.Count} rows to write");
            Console.WriteLine($"Mock data has {_mockDataRead.Count} rows to read");
            Console.WriteLine();

            // Run benchmarks for given databases
            var databases = _config.GetSection("Databases").GetChildren();
            foreach(var database in databases)
            {
                var configs = database.GetChildren();
                var type = configs.FirstOrDefault(x => x.Key == "Type").Value;
                switch (type)
                {
                    case "Fake":
                        FakeDb fakeDb = new FakeDb();
                        RunBenchmark(database.Key, fakeDb);
                        break;
                    case "Sql":
                        var sqlConnectionString = configs.FirstOrDefault(x => x.Key == "ConnectionString").Value;
                        var sqlTableName = configs.FirstOrDefault(x => x.Key == "TableName").Value;
                        SqlDb sqlDb = new SqlDb(sqlConnectionString, sqlTableName);
                        RunBenchmark(database.Key, sqlDb);
                        break;
                    case "Postgre":
                        var pgConnectionString = configs.FirstOrDefault(x => x.Key == "ConnectionString").Value;
                        var pgTableName = configs.FirstOrDefault(x => x.Key == "TableName").Value;
                        PostgreDb pgDb = new PostgreDb(pgConnectionString, pgTableName);
                        RunBenchmark(database.Key, pgDb);
                        break;
                    case "DynamoDb":
                        var dynId = configs.FirstOrDefault(x => x.Key == "AccessId").Value;
                        var dynKey = configs.FirstOrDefault(x => x.Key == "SecretKey").Value;
                        var dynTableName = configs.FirstOrDefault(x => x.Key == "TableName").Value;
                        var dynCapcity = configs.FirstOrDefault(x => x.Key == "Capacity").GetChildren();
                        var dynSize = Convert.ToInt32(dynCapcity.FirstOrDefault(x => x.Key == "Size").Value);
                        var dynWrite = Convert.ToInt32(dynCapcity.FirstOrDefault(x => x.Key == "Write").Value);
                        var dynRead = Convert.ToInt32(dynCapcity.FirstOrDefault(x => x.Key == "Read").Value);
                        DynamoDb dynDb = new DynamoDb(dynTableName, dynId, dynKey, dynSize, dynWrite, dynRead);
                        RunBenchmark(database.Key, dynDb);
                        break;
                    default:
                        Console.WriteLine($"ERROR - Invalid database type {type}");
                        break;
                }
            }

            Console.WriteLine();
            Console.WriteLine("Benchmark Completed");
            Console.WriteLine();
        }

        private static void RunBenchmark(string name, Db database)
        {
            Console.WriteLine(name);
            Console.WriteLine("------------------------------");
            Stopwatch watch = new Stopwatch();

            Console.Write($"Resetting tables".PadRight(20, '.'));
            watch.Start();
            database.ResetTable().Wait();
            watch.Stop();
            Console.WriteLine($"Completed! Time: {watch.Elapsed.TotalSeconds}");
            watch.Reset();

            Console.Write($"Writing data".PadRight(20, '.'));
            watch.Start();
            database.Write(_mockDataWrite).Wait();
            watch.Stop();
            Console.WriteLine($"Completed! Time: {watch.Elapsed.TotalSeconds}");
            watch.Reset();

            Console.Write($"Reading data".PadRight(20, '.'));
            watch.Start();
            database.Read(_mockDataRead).Wait();
            watch.Stop();
            Console.WriteLine($"Completed! Time: {watch.Elapsed.TotalSeconds}");
            watch.Reset();
            Console.WriteLine();
        }
    }
}
