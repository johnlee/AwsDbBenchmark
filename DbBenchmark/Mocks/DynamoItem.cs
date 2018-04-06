﻿using Amazon.DynamoDBv2.DataModel;

namespace DbBenchmark.Mocks
{
    [DynamoDBTable("Benchmark")]
    public class DynamoItem
    {
        [DynamoDBHashKey]
        public int Id { get; set; }
        public string Column1 { get; set; }
        public string Column2 { get; set; }
        public string Column3 { get; set; }
        public string Column4 { get; set; }
        public string Column5 { get; set; }
        public string Column6 { get; set; }
        public string Column7 { get; set; }
        public string Column8 { get; set; }
        public string Column9 { get; set; }
    }
}
