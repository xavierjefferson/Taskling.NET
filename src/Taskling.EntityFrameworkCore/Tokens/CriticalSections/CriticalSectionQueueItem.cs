using Newtonsoft.Json;

namespace Taskling.SqlServer.Tokens.CriticalSections;

public class CriticalSectionQueueItem
{
    public CriticalSectionQueueItem()
    {
    }
    [JsonProperty("D")]
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public CriticalSectionQueueItem(long taskExecutionId)
    {
        TaskExecutionId = taskExecutionId;
    }

    [JsonProperty("T")]
    public long TaskExecutionId { get; set; }
}