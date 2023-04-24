using System;
using System.Collections.Generic;

namespace Taskling.Models123
{
    public partial class ForceBlockQueue
    {
        public int ForceBlockQueueId { get; set; }
        public long BlockId { get; set; }
        public DateTime ForcedDate { get; set; }
        public string ForcedBy { get; set; }
        public string ProcessingStatus { get; set; }

        public virtual Block Block { get; set; }
    }
}
