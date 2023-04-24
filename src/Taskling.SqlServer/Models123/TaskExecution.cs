using System;
using System.Collections.Generic;

namespace Taskling.Models123
{
    public partial class TaskExecution
    {
        public TaskExecution()
        {
            BlockExecutions = new HashSet<BlockExecution>();
            TaskExecutionEvents = new HashSet<TaskExecutionEvent>();
        }

        public int TaskExecutionId { get; set; }
        public int TaskDefinitionId { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public DateTime LastKeepAlive { get; set; }
        public string ServerName { get; set; }
        public int TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
        public int FailedTaskRetryLimit { get; set; }
        public int DeadTaskRetryLimit { get; set; }
        public string ReferenceValue { get; set; }
        public bool Failed { get; set; }
        public bool Blocked { get; set; }
        public string TasklingVersion { get; set; }
        public string ExecutionHeader { get; set; }

        public virtual TaskDefinition TaskDefinition { get; set; }
        public virtual ICollection<BlockExecution> BlockExecutions { get; set; }
        public virtual ICollection<TaskExecutionEvent> TaskExecutionEvents { get; set; }
    }
}
