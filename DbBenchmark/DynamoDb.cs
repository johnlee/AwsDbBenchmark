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

        public DynamoDb(string tablename, string accessId, string secretKey)
        {
            AWSCredentials credentials = new BasicAWSCredentials(accessId, secretKey);
            _tablename = tablename;
            _client = new AmazonDynamoDBClient(credentials, Amazon.RegionEndpoint.USGovCloudWest1);
            _context = new DynamoDBContext(_client);
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
                        ReadCapacityUnits = 3,
                        WriteCapacityUnits = 5
                    }
                };

                var respCreate = await _client.CreateTableAsync(request);
                System.Threading.Thread.Sleep(10000); // Give AWS some time to actually create the table
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
            }
        }

        public async Task Write(List<Item> items)
        {
            var dynamoItems = ConvertToDynamo(items);
            try
            {
                // Break down the writes to 500 chunks with sleeps inbetween
                var index = 0;
                var totalRetries = 0;
                var chunk = dynamoItems.GetRange(index, 500);
                while(chunk.Count > 0 && totalRetries < 10)
                {
                    try
                    {
                        var batchWrite = _context.CreateBatchWrite<DynamoItem>();
                        batchWrite.AddPutItems(chunk);
                        await batchWrite.ExecuteAsync();
                        System.Threading.Thread.Sleep(3000);
                        index += 500;
                        chunk = dynamoItems.GetRange(index, 500);
                    }
                    catch (Exception e)
                    {
                        totalRetries++;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"ERROR - {e.Message}");
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
                Console.WriteLine($"ERROR - {e.Message}");
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
            foreach(var item in items)
            {
                result.Add(new DynamoItem {
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