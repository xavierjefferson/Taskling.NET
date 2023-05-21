using System;
using Taskling.Blocks.Common;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindBlocksOfTaskRequest : BlockRequestBase
{
    public FindBlocksOfTaskRequest(TaskId taskId,
        long taskExecutionId,
        BlockType blockType,
        Guid referenceValueOfTask,
        ReprocessOption reprocessOption)
        : base(taskId, taskExecutionId, blockType)
    {
        ReferenceValueOfTask = referenceValueOfTask;
        ReprocessOption = reprocessOption;
    }

    public Guid ReferenceValueOfTask { get; set; }
    public ReprocessOption ReprocessOption { get; set; }
}