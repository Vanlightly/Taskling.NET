using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.Requests;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.Blocks.Factories
{
    public interface IBlockFactory
    {
        Task<IList<IDateRangeBlockContext>> GenerateDateRangeBlocksAsync(DateRangeBlockRequest blockRequest);
        Task<IList<INumericRangeBlockContext>> GenerateNumericRangeBlocksAsync(NumericRangeBlockRequest blockRequest);
        Task<IList<IListBlockContext<T>>> GenerateListBlocksAsync<T>(ListBlockRequest blockRequest);
        Task<IList<IListBlockContext<TItem, THeader>>> GenerateListBlocksAsync<TItem, THeader>(ListBlockRequest blockRequest);
        Task<IList<IObjectBlockContext<T>>> GenerateObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest);
        
        Task<IListBlock<T>> GetLastListBlockAsync<T>(LastBlockRequest lastBlockRequest);
        Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>(LastBlockRequest lastBlockRequest);
    }
}
