using System;
using System.Collections.Generic;

namespace Taskling.Models123
{
    public partial class TaskExecutionEvent
    {
        public long TaskExecutionEventId { get; set; }
        public int TaskExecutionId { get; set; }
        public int EventType { get; set; }
        public string Message { get; set; }
        public DateTime EventDateTime { get; set; }

        public virtual TaskExecution TaskExecution { get; set; }
    }
}
