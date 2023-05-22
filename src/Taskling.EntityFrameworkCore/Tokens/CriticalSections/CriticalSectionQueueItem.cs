using Newtonsoft.Json;

namespace Taskling.EntityFrameworkCore.Tokens.CriticalSections;

public class CriticalSectionQueueItem
{
    public CriticalSectionQueueItem()
    {
    }

    public CriticalSectionQueueItem(long taskExecutionId)
    {
        TaskExecutionId = taskExecutionId;
    }

    [JsonProperty("D")] public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    [JsonProperty("T")] public long TaskExecutionId { get; set; }
}