using DbBenchmark.Mocks;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DbBenchmark
{
    public interface Db
    {
        Task ResetTable();
        Task Write(List<Item> items);
        Task Read(List<Item> items);
    }
}
