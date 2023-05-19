using System;
using Newtonsoft.Json;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling.Blocks.Requests;

public abstract class BlockRequest
{
    public BlockRequest(TaskId taskId)
    {
        TaskId = taskId;
    }

    [JsonProperty(Order = 900)] public TaskId TaskId { get; }

    //public int TaskExecutionId { get; set; }

    //public int MaxBlocks { get; set; }

    //public bool ReprocessFailedTasks { get; set; }
    //public TimeSpan FailedTaskDetectionRange { get; set; }
    //public int FailedTaskRetryLimit { get; set; }

    //public TaskDeathMode TaskDeathMode { get; set; }
    //public bool ReprocessDeadTasks { get; set; }
    //public int DeadTaskRetryLimit { get; set; }
    //public TimeSpan OverrideDeathThreshold { get; set; }
    //public TimeSpan DeadTaskDetectionRange { get; set; }
    //public TimeSpan KeepAliveDeathThreshold { get; set; }

    //public BlockType BlockType { get; protected set; }

    //public Guid ReprocessReferenceValue { get; set; }
    //public ReprocessOption ReprocessOption { get; set; }

    [JsonProperty(Order = 100)] public BlockType BlockType { get; protected set; }

    [JsonProperty(Order = 200)] public bool ReprocessDeadTasks { get; set; }

    [JsonProperty(Order = 300)] public bool ReprocessFailedTasks { get; set; }

    [JsonProperty(Order = 400)] public int MaxBlocks { get; set; }

    [JsonProperty(Order = 500)] public int TaskExecutionId { get; set; }

    [JsonProperty(Order = 600)] public ReprocessOption ReprocessOption { get; set; }

    [JsonProperty(Order = 700)] public int DeadTaskRetryLimit { get; set; }

    [JsonProperty(Order = 800)] public int FailedTaskRetryLimit { get; set; }

    //[JsonProperty(Order = 900)]
    //public string ApplicationName { get; set; }
    [JsonProperty(Order = 1000)] public Guid ReprocessReferenceValue { get; set; }

    //[JsonProperty(Order = 1100)]
    //public string TaskName { get; set; }
    [JsonProperty(Order = 1200)] public TaskDeathMode TaskDeathMode { get; set; }

    [JsonProperty(Order = 1300)] public TimeSpan DeadTaskDetectionRange { get; set; }

    [JsonProperty(Order = 1400)] public TimeSpan FailedTaskDetectionRange { get; set; }

    [JsonProperty(Order = 1500)] public TimeSpan KeepAliveDeathThreshold { get; set; }

    [JsonProperty(Order = 1600)] public TimeSpan OverrideDeathThreshold { get; set; }
}