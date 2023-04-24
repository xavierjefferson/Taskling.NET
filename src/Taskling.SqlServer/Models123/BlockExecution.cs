using System;
using System.Collections.Generic;

namespace Taskling.Models123
{
    public partial class BlockExecution
    {
        public long BlockExecutionId { get; set; }
        public int TaskExecutionId { get; set; }
        public long BlockId { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public int Attempt { get; set; }
        public int? ItemsCount { get; set; }
        public byte BlockExecutionStatus { get; set; }

        public virtual TaskExecution TaskExecution { get; set; }
    }
}
