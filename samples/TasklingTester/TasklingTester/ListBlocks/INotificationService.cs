using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TasklingTester.Entities;

namespace TasklingTester.ListBlocks
{
    public interface INotificationService
    {
        void NotifyUser(TravelInsight travelInsight);
    }
}
