using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindBlocksOfTaskRequest : BlockRequestBase
{
    public FindBlocksOfTaskRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
        Guid referenceValueOfTask,
        ReprocessOptionEnum reprocessOptionEnum)
        : base(taskId, taskExecutionId, blockType)
    {
        ReferenceValueOfTask = referenceValueOfTask;
        ReprocessOption = reprocessOptionEnum;
    }

    public Guid ReferenceValueOfTask { get; set; }
    public ReprocessOptionEnum ReprocessOption { get; set; }
}