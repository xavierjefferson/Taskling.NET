using System;
using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public class TaskConfiguration : ConfigurationOptions
{
    public TaskConfiguration(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }
    public DateTime DateLoaded { get; set; }
}