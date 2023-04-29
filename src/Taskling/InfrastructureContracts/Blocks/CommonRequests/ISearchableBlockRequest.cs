using System;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public interface ISearchableBlockRequest : IBlockRequest
{
    int BlockCountLimit { get; }
    int RetryLimit { get; }
    int[] GetMatchingStatuses();

    public int AttemptLimit { get; }
    public DateTime SearchPeriodBegin { get; set; }
    public DateTime SearchPeriodEnd { get; set; }
}