using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TasklingTesterAsync.Entities;

namespace TasklingTesterAsync.ListBlocks
{
    public interface INotificationService
    {
        Task NotifyUserAsync(TravelInsight travelInsight);
    }
}
