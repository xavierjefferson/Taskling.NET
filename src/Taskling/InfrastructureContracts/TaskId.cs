using System;

namespace Taskling.InfrastructureContracts;

public class TaskId
{
    public TaskId(string applicationName, string taskName)
    {
        ApplicationName = applicationName;
        TaskName = taskName;
    }

    public string ApplicationName { get; }
    public string TaskName { get; }

    public string GetUniqueKey()
    {
        return ApplicationName + "::" + TaskName;
    }

    public override bool Equals(object obj)
    {
        if (obj == null)
            return false;

        var taskId = obj as TaskId;
        if (taskId == null)
            return false;

        return taskId.ApplicationName == ApplicationName
               && taskId.TaskName == TaskName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(ApplicationName, TaskName);
    }
}