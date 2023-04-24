﻿using System;
using System.Threading.Tasks;

namespace Taskling.InfrastructureContracts.TaskExecution;

public interface ITaskRepository
{
    Task<TaskDefinition> EnsureTaskDefinitionAsync(TaskId taskId);
    Task<DateTime> GetLastTaskCleanUpTimeAsync(TaskId taskId);
    Task SetLastCleanedAsync(TaskId taskId);
}