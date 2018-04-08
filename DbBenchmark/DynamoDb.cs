using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DbBenchmark.Mocks;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace DbBenchmark
{
    public class DynamoDb : Db
    {
        private AmazonDynamoDBClient _client;
        private DynamoDBContext _context;
        private string _tablename;
        private int _sizeCapacity;
        private int _writeCapacity;
        private int _readCapacity;

        public DynamoDb(string tablename, string accessId, string secretKey, int size, int write, int read)
        {
            AWSCredentials credentials = new BasicAWSCredentials(accessId, secretKey);
            _tablename = tablename;
            _client = new AmazonDynamoDBClient(credentials, Amazon.RegionEndpoint.USGovCloudWest1);
            _context = new DynamoDBContext(_client);
            _sizeCapacity = size;
            _writeCapacity = write;
            _readCapacity = read;
        }

        public async Task ResetTable()
        {
            try
            {
                var tables = await _client.ListTablesAsync();

                // Delete the table
                if (tables.TableNames.Contains(_tablename))
                {
                    var respDelete = await _client.DeleteTableAsync(_tablename);
                    if (respDelete.HttpStatusCode == HttpStatusCode.OK)
                    {
                        System.Threading.Thread.Sleep(10000); // Give AWS some time to actually delete the table
                    }
                    else
                    {
                        throw new Exception($"Could not delete table {_tablename}. Status code: {respDelete.HttpStatusCode}");
                    }
                }

                // Create the table
                var request = new CreateTableRequest
                {
                    TableName = _tablename,
                    KeySchema = new List<KeySchemaElement>()
                    {
                        new KeySchemaElement
                        {
                            AttributeName = "Id",
                            KeyType = KeyType.HASH
                        }
                    },
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition
                        {
                            AttributeName = "Id",
                            AttributeType = ScalarAttributeType.N
                        }
                    },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = _readCapacity,
                        WriteCapacityUnits = _writeCapacity
                    }
                };

                var respCreate = await _client.CreateTableAsync(request);
                System.Threading.Thread.Sleep(10000); // Give AWS some time to actually create the table
            }
            catch (Exception e)
            {
                Console.WriteLine($"  {DateTime.Now}: ERROR! {e.Message}");
            }
        }

        public async Task Write(List<Item> items)
        {
            if (items.Count < 1) { return; }
            var dynamoItems = ConvertToDynamo(items);

            // Calculate the write rate
            decimal sizeKb = _sizeCapacity * 1000;
            var rowSize = (int)Math.Round(sizeKb / dynamoItems[0].ToString().Length) * 10;
            var chunkSize = rowSize * _writeCapacity;

            var index = 0;
            var retryMax = 10;
            var retryCount = 0;
            var retryWait = 1000;
            var chunk = dynamoItems.GetRange(index, chunkSize);
            while (chunk.Count > 0)
            {
                try
                {
                    var batchWrite = _context.CreateBatchWrite<DynamoItem>();
                    batchWrite.AddPutItems(chunk);
                    await batchWrite.ExecuteAsync();
                    System.Threading.Thread.Sleep(1000); // write per second

                    // Reset for next write
                    index += chunkSize;
                    chunk = dynamoItems.GetRange(index, chunkSize);
                    retryCount = 0;
                    retryWait = 1000;
                }
                catch (ProvisionedThroughputExceededException)
                {
                    if (retryCount < retryMax)
                    {
                        retryCount++;
                        retryWait = retryWait * 2;
                        Console.WriteLine($"  {DateTime.Now}: ERROR! Exceeded write capcity. At chunk: {index} Retry count at: {retryCount} Retrying in: {retryWait} ms");
                        System.Threading.Thread.Sleep(retryWait);
                    }
                    else
                    {
                        Console.WriteLine($"  {DateTime.Now}: ERROR! Exceeded maximum retry. Aborting write process!");
                        return;
                    }
                }
                catch (ArgumentException)
                {
                    // Load the last remaining chunk
                    chunkSize = dynamoItems.Count - index;
                    chunk = dynamoItems.GetRange(index, chunkSize);
                }
            }
        }

        public async Task Read(List<Item> items)
        {
            var dynamoItems = ConvertToDynamo(items);
            try
            {
                foreach (var item in dynamoItems)
                {
                    var read = await _context.LoadAsync<DynamoItem>(item.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"  {DateTime.Now}: ERROR! {e.Message}");
            }
        }

        public async Task<List<DynamoItem>> Scan()
        {
            var scanConditions = new List<ScanCondition>
                {
                    new ScanCondition("Swid", ScanOperator.GreaterThan, 0)
                };
            var scan = _context.ScanAsync<DynamoItem>(scanConditions);
            return await scan.GetRemainingAsync();
        }

        private List<DynamoItem> ConvertToDynamo(List<Item> items)
        {
            var result = new List<DynamoItem>();
            foreach (var item in items)
            {
                result.Add(new DynamoItem
                {
                    Id = item.Id,
                    Column1 = item.Column1,
                    Column2 = item.Column2,
                    Column3 = item.Column3,
                    Column4 = item.Column4,
                    Column5 = item.Column5,
                    Column6 = item.Column6,
                    Column7 = item.Column7,
                    Column8 = item.Column8,
                    Column9 = item.Column9
                });
            }
            return result;
        }
    }
}