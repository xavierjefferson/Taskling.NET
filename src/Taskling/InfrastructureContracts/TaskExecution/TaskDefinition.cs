﻿namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskDefinition
{
    public int TaskDefinitionId { get; set; }
    public string ApplicationName { get; set; }
    public string TaskName { get; set; }
}