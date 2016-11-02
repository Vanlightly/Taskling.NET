using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.Requests;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.Blocks.Factories
{
    public interface IBlockFactory
    {
        IList<IDateRangeBlockContext> GenerateDateRangeBlocks(DateRangeBlockRequest blockRequest);
        IList<INumericRangeBlockContext> GenerateNumericRangeBlocks(NumericRangeBlockRequest blockRequest);
        IList<IListBlockContext<T>> GenerateListBlocks<T>(ListBlockRequest blockRequest);
        IList<IListBlockContext<TItem, THeader>> GenerateListBlocks<TItem, THeader>(ListBlockRequest blockRequest);
        IList<IObjectBlockContext<T>> GenerateObjectBlocks<T>(ObjectBlockRequest<T> blockRequest);

        IListBlock<T> GetLastListBlock<T>(LastBlockRequest lastBlockRequest);
        IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>(LastBlockRequest lastBlockRequest);
    }
}
