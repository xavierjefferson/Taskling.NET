using Taskling.Blocks.Common;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindBlocksOfTaskRequest : BlockRequestBase
{
    public FindBlocksOfTaskRequest(TaskId taskId,
        int taskExecutionId,
        BlockType blockType,
        string referenceValueOfTask,
        ReprocessOption reprocessOption)
        : base(taskId, taskExecutionId, blockType)
    {
        ReferenceValueOfTask = referenceValueOfTask;
        ReprocessOption = reprocessOption;
    }

    public string ReferenceValueOfTask { get; set; }
    public ReprocessOption ReprocessOption { get; set; }
}