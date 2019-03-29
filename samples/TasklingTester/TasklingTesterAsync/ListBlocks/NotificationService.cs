using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync.ListBlocks
{
    public class NotificationService : INotificationService
    {
        public async Task NotifyUserAsync(TravelInsight travelInsight)
        {
            // send a push notification or something
            await Task.Yield();
        }
    }
}
