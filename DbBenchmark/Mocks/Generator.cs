using System;
using System.Collections.Generic;
using System.Linq;

namespace DbBenchmark.Mocks
{
    public class Generator
    {
        private static Random _random;
        private int _length;

        public Generator(int length = 10000)
        {
            _random = new Random();
            _length = length;
        }

        public List<Item> GenerateData()
        {
            var result = new List<Item>();
            for (int i = 0; i < _length; i++)
            {
                Item item = new Item();
                item.Id = i;
                item.Column1 = RandomString();
                item.Column2 = RandomString();
                item.Column3 = RandomString();
                item.Column4 = RandomString();
                item.Column5 = RandomString();
                item.Column6 = RandomString();
                item.Column7 = RandomString();
                item.Column8 = RandomString();
                item.Column9 = RandomString();
                result.Add(item);
            }
            return result;
        }

        public List<Item> SelectRandoms(List<Item> items, int count)
        {
            var result = new List<Item>();
            for(int i = 0; i < count; i++)
            {
                var index = _random.Next(items.Count);
                result.Add(items[index]);
            }
            return result;
        }

        private static string RandomString()
        {
            const string chars = "abcdefghijklmn opqrstuvwxy ABCDEFGHIJKLMN OPQRSTUVWXYZ 0123456789";
            int length = _random.Next(40, 50);
            return new string(Enumerable.Repeat(chars, length).Select(s => s[_random.Next(s.Length)]).ToArray());
        }
    }
}
