using System;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CleanUp;

public class CleanUpRequest : RequestBase
{
    public DateTime GeneralDateThreshold { get; set; }
    public DateTime ListItemDateThreshold { get; set; }
    public TimeSpan TimeSinceLastCleaningThreashold { get; set; }
}