﻿namespace Taskling.CleanUp;

public interface ICleanUpService
{
    void CleanOldData(string applicationName, string taskName, int taskExecutionId);
}