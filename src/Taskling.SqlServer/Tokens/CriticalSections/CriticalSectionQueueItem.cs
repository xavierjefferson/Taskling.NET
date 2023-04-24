namespace Taskling.SqlServer.Tokens.CriticalSections;

internal class CriticalSectionQueueItem
{
    public CriticalSectionQueueItem()
    {
    }

    public CriticalSectionQueueItem(int index, int taskExecutionId)
    {
        Index = index;
        TaskExecutionId = taskExecutionId;
    }

    public int Index { get; set; }
    public int TaskExecutionId { get; set; }
}