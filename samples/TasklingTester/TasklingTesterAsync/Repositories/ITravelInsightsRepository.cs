using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync.Repositories
{
    public interface ITravelInsightsRepository
    {
        Task AddAsync(IList<TravelInsight> insights);
    }
}
