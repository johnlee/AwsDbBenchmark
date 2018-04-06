using DbBenchmark.Mocks;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DbBenchmark
{
    public class FakeDb : Db
    {
        public FakeDb() { }

        public async Task ResetTable()
        {
            await Task.Delay(1000);
        }

        public async Task Write(List<Item> items)
        {
            await Task.Delay(1000);
        }

        public async Task Read(List<Item> items)
        {
            await Task.Delay(1000);
        }
    }
}
