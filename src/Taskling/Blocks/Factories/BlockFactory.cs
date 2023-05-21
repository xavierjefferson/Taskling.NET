using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Blocks.Requests;
using Taskling.Contexts;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;
using Taskling.Serialization;

namespace Taskling.Blocks.Factories;

public class BlockFactory : IBlockFactory
{
    private readonly IBlockRepository _blockRepository;
    private readonly IListBlockRepository _listBlockRepository;
    private readonly ILogger<BlockFactory> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IObjectBlockRepository _objectBlockRepository;
    private readonly IRangeBlockRepository _rangeBlockRepository;
    private readonly IRetryService _retryService;
    private readonly IServiceProvider _serviceProvider;
    private readonly ITaskExecutionRepository _taskExecutionRepository;

    public BlockFactory(IBlockRepository blockRepository,
        IRangeBlockRepository rangeBlockRepository,
        IListBlockRepository listBlockRepository,
        IObjectBlockRepository objectBlockRepository,
        ITaskExecutionRepository taskExecutionRepository, ILoggerFactory loggerFactory, IRetryService retryService,
        IServiceProvider serviceProvider)
    {
        _logger = loggerFactory.CreateLogger<BlockFactory>();
        _blockRepository = blockRepository;
        _rangeBlockRepository = rangeBlockRepository;
        _listBlockRepository = listBlockRepository;
        _objectBlockRepository = objectBlockRepository;
        _taskExecutionRepository = taskExecutionRepository;
        _loggerFactory = loggerFactory;
        _retryService = retryService;

        _serviceProvider = serviceProvider;
    }

    public async Task<IList<IDateRangeBlockContext>> GenerateDateRangeBlocksAsync(DateRangeBlockRequest blockRequest)
    {
        var blocks =
            await GenerateRangeBlocksAsync(blockRequest, i => i.RangeBegin != null, GenerateNewDateRangeBlocksAsync);

   

        var dateRangeBlocks = blocks.Select(x => (IDateRangeBlockContext)x);
        var tmp = dateRangeBlocks.OrderBy(x => x.DateRangeBlock.RangeBlockId).ToList();
        _logger.Debug($"Returning {tmp.Count} items");
        return tmp;
    }

    public async Task<IList<INumericRangeBlockContext>> GenerateNumericRangeBlocksAsync(
        NumericRangeBlockRequest blockRequest)
    {
        var blocks = await GenerateRangeBlocksAsync(blockRequest, i => i.RangeBegin != null,
            GenerateNewNumericRangeBlocksAsync);

        var numericRangeBlocks = blocks.Select(x => (INumericRangeBlockContext)x);
        return numericRangeBlocks.OrderBy(x => x.NumericRangeBlock.RangeBlockId).ToList();
    }

    public async Task<IList<IListBlockContext<T>>> GenerateListBlocksAsync<T>(ListBlockRequest blockRequest)
    {
        var blocks = await CreateProtoListBlocksAsync(blockRequest).ConfigureAwait(false);
        var blockContexts =
            (await CreateListBlockContextsAsync<T>(blockRequest, blocks).ConfigureAwait(false)).ToList();

        if (blocks.Any(x => x.IsForcedBlock))
            await DequeueForcedBlocksAsync(blockRequest,
                blocks.Where(x => x.IsForcedBlock).Select(x => x.ForcedBlockQueueId).ToList()).ConfigureAwait(false);

        if (!blocks.Any())
            await LogEmptyBlockEventAsync(blockRequest.TaskExecutionId, blockRequest.TaskId).ConfigureAwait(false);

        return blockContexts.OrderBy(x => x.Block.ListBlockId).ToList();
    }

    public async Task<IList<IListBlockContext<TItem, THeader>>> GenerateListBlocksAsync<TItem, THeader>(
        ListBlockRequest blockRequest)
    {
        var blocks = await CreateProtoListBlocksAsync(blockRequest).ConfigureAwait(false);
        var blockContexts =
            (await CreateListBlockContextsAsync<TItem, THeader>(blockRequest, blocks).ConfigureAwait(false)).ToList();

        if (blocks.Any(x => x.IsForcedBlock))
            await DequeueForcedBlocksAsync(blockRequest,
                blocks.Where(x => x.IsForcedBlock).Select(x => x.ForcedBlockQueueId).ToList()).ConfigureAwait(false);

        if (!blocks.Any())
            await LogEmptyBlockEventAsync(blockRequest.TaskExecutionId, blockRequest.TaskId).ConfigureAwait(false);

        return blockContexts.OrderBy(x => x.Block.ListBlockId).ToList();
    }

    public async Task<IList<IObjectBlockContext<T>>> GenerateObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest)
    {
        var blocks = new List<IObjectBlockContext<T>>();

        if (blockRequest.ReprocessReferenceValue != Guid.Empty)
        {
            blocks = await LoadObjectBlocksOfTaskAsync(blockRequest).ConfigureAwait(false);
        }
        else
        {
            var forceBlocks = await GetForcedObjectBlocksAsync(blockRequest).ConfigureAwait(false);
            blocks.AddRange(forceBlocks);

            if (GetBlocksRemaining(blockRequest, blocks) > 0)
                await LoadFailedAndDeadObjectBlocksAsync(blockRequest, blocks).ConfigureAwait(false);

            if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.Object != null)
            {
                var newBlock = await GenerateNewObjectBlockAsync(blockRequest).ConfigureAwait(false);
                blocks.Add(newBlock);
            }
        }

        if (!blocks.Any())
            await LogEmptyBlockEventAsync(blockRequest.TaskExecutionId, blockRequest.TaskId).ConfigureAwait(false);

        return blocks.OrderBy(x => x.Block.ObjectBlockId).ToList();
    }

    public async Task<IListBlock<T>> GetLastListBlockAsync<T>(LastBlockRequest lastBlockRequest)
    {
        var lastProtoListBlock =
            await _listBlockRepository.GetLastListBlockAsync(lastBlockRequest).ConfigureAwait(false);
        if (lastProtoListBlock == null)
            return null;

        return Convert<T>(lastProtoListBlock, true);
    }

    public async Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>(
        LastBlockRequest lastBlockRequest)
    {
        var lastProtoListBlock =
            await _listBlockRepository.GetLastListBlockAsync(lastBlockRequest).ConfigureAwait(false);

        return Convert<TItem, THeader>(lastProtoListBlock, true);
    }

    public async Task<List<RangeBlockContext>> GenerateRangeBlocksAsync<T>(T blockRequest, Func<T, bool> testadd,
        Func<T, int, Task<List<RangeBlockContext>>> newBlockFunc) where T : BlockRequest
    {
        var blocks = new List<RangeBlockContext>();

        if (blockRequest.ReprocessReferenceValue != Guid.Empty)
        {
            _logger.LogDebug($"{nameof(blockRequest.ReprocessReferenceValue)} is not null or empty");
            blocks = await LoadRangeBlocksOfTaskAsync(blockRequest).ConfigureAwait(false);
            _logger.LogDebug($"{blocks.Count} were fetched at f70d5f71-f246-4c2c-82e8-71d3438382f2.");
        }
        else
        {
            _logger.LogDebug($"{nameof(blockRequest.ReprocessReferenceValue)} is null or empty.");
            _logger.LogDebug("Getting forced blocks...");
            var forceBlocks = await GetForcedBlocksAsync(blockRequest).ConfigureAwait(false);
            blocks.AddRange(forceBlocks);

            for (var iteration = 0; iteration < 3; iteration++)
            {
                var remaining = GetBlocksRemaining(blockRequest, blocks);
                if (remaining == 0)
                {
                    _logger.LogDebug("No more blocks need to be fetched");
                    break;
                }

                switch (iteration)
                {
                    case 0:
                        if (blockRequest.ReprocessDeadTasks)
                        {
                            _logger.LogDebug("loading dead blocks");
                            var rangeBlockContexts =
                                await GetDeadBlocksAsync(blockRequest, remaining).ConfigureAwait(false);
                            _logger.LogDebug($"{rangeBlockContexts.Count} dead blocks were fetched");
                            blocks.AddRange(rangeBlockContexts);
                        }
                        else
                        {
                            _logger.LogDebug("Skipping dead blocks");
                        }

                        break;
                    case 1:
                        if (blockRequest.ReprocessFailedTasks)
                        {
                            _logger.LogDebug("loading failed blocks");
                            var rangeBlockContexts = await GetFailedBlocksAsync(blockRequest, remaining)
                                .ConfigureAwait(false);
                            _logger.LogDebug($"{rangeBlockContexts.Count} failed blocks were fetched");
                            blocks.AddRange(rangeBlockContexts);
                        }
                        else
                        {
                            _logger.LogDebug("Skipping failed blocks");
                        }

                        break;
                    case 2:
                        var willAddNewBlocks = testadd(blockRequest);
                        if (willAddNewBlocks)
                        {
                            _logger.LogDebug("Generating new range");
                            blocks.AddRange(await newBlockFunc(blockRequest, remaining).ConfigureAwait(false));
                        }
                        else
                        {
                            _logger.LogDebug("skipping new range generation");
                        }

                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        if (!blocks.Any())
        {
            _logger.LogDebug("No blocks were pulled");
            await LogEmptyBlockEventAsync(blockRequest.TaskExecutionId, blockRequest.TaskId).ConfigureAwait(false);
        }
        else
        {
            _logger.LogDebug($"{nameof(GenerateRangeBlocksAsync)} is returning {blocks.Count} blocks");
        }

        return blocks;
    }


    private FindDeadBlocksRequest CreateDeadBlocksRequest(BlockRequest blockRequest, int blockCountLimit)
    {
        var utcNow = DateTime.UtcNow;

        return new FindDeadBlocksRequest(blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            utcNow - blockRequest.DeadTaskDetectionRange,
            utcNow.AddMinutes(-1),
            blockCountLimit,
            blockRequest.TaskDeathMode,
            blockRequest.DeadTaskRetryLimit);
    }


    private async Task LogEmptyBlockEventAsync(long taskExecutionId, TaskId taskId)
    {
        var checkPointRequest = new TaskExecutionCheckpointRequest(taskId)
        {
            TaskExecutionId = taskExecutionId,
            Message = "No values for generate the block. Emtpy Block context returned."
        };
        await _taskExecutionRepository.CheckpointAsync(checkPointRequest).ConfigureAwait(false);
    }


    private int GetBlocksRemaining<T>(BlockRequest blockRequest, List<T> blocks)
    {
        return blockRequest.MaxBlocks - blocks.Count;
    }

    private async Task<List<RangeBlockContext>> GetForcedBlocksAsync(BlockRequest blockRequest)
    {
        var forcedBlockRequest = new QueuedForcedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType);

        var queuedForcedBlocks =
            await _blockRepository.GetQueuedForcedRangeBlocksAsync(forcedBlockRequest).ConfigureAwait(false);

        var forcedBlocks = new List<RangeBlockContext>();
        foreach (var queuedForcedBlock in queuedForcedBlocks)
            forcedBlocks.Add(await CreateBlockContextAsync(blockRequest, queuedForcedBlock.RangeBlock,
                queuedForcedBlock.ForcedBlockQueueId).ConfigureAwait(false));

        if (forcedBlocks.Any())
            await DequeueForcedBlocksAsync(blockRequest, forcedBlocks.Select(x => x.ForcedBlockQueueId).ToList())
                .ConfigureAwait(false);

        return forcedBlocks;
    }

    private async Task DequeueForcedBlocksAsync(BlockRequest blockRequest, List<int> forcedBlockQueueIds)
    {
        var request = new DequeueForcedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            forcedBlockQueueIds);

        await _blockRepository.DequeueForcedBlocksAsync(request).ConfigureAwait(false);
    }

    private async Task<List<RangeBlockContext>> LoadRangeBlocksOfTaskAsync(BlockRequest blockRequest)
    {
        var failedBlockRequest = new FindBlocksOfTaskRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            blockRequest.ReprocessReferenceValue,
            blockRequest.ReprocessOption);

        var blocksOfTask = (await _blockRepository.FindRangeBlocksOfTaskAsync(failedBlockRequest).ConfigureAwait(false))
            .ToList();
        if (!blocksOfTask.Any())
            return new List<RangeBlockContext>();

        return await CreateBlockContextsAsync(blockRequest, blocksOfTask).ConfigureAwait(false);
    }

    private async Task<IList<ProtoListBlock>> LoadListBlocksOfTaskAsync(ListBlockRequest blockRequest)
    {
        var failedBlockRequest = new FindBlocksOfTaskRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            blockRequest.ReprocessReferenceValue,
            blockRequest.ReprocessOption);

        var blocksOfTask = await _blockRepository.FindListBlocksOfTaskAsync(failedBlockRequest).ConfigureAwait(false);

        return blocksOfTask;
    }

    private async Task<List<RangeBlockContext>> GetDeadBlocksAsync(BlockRequest blockRequest, int blockCountLimit)
    {
        var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
        var deadBlocks = await _blockRepository.FindDeadRangeBlocksAsync(deadBlockRequest).ConfigureAwait(false);
        return await CreateBlockContextsAsync(blockRequest, deadBlocks).ConfigureAwait(false);
    }

    private async Task<List<RangeBlockContext>> GetFailedBlocksAsync(BlockRequest blockRequest, int blockCountLimit)
    {
        var failedBlockRequest = new FindFailedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
            DateTime.UtcNow,
            blockCountLimit,
            blockRequest.FailedTaskRetryLimit
        );

        var failedBlocks = await _blockRepository.FindFailedRangeBlocksAsync(failedBlockRequest).ConfigureAwait(false);
        return await CreateBlockContextsAsync(blockRequest, failedBlocks).ConfigureAwait(false);
    }

    private async Task<List<RangeBlockContext>> CreateBlockContextsAsync(BlockRequest blockRequest,
        IList<RangeBlock> rangeBlocks)
    {
        var blocks = new List<RangeBlockContext>();
        foreach (var rangeBlock in rangeBlocks)
        {
            var blockContext = await CreateBlockContextAsync(blockRequest, rangeBlock).ConfigureAwait(false);
            blocks.Add(blockContext);
        }

        return blocks;
    }

    private async Task<RangeBlockContext> CreateBlockContextAsync(BlockRequest blockRequest,
        RangeBlock rangeBlock,
        int forcedBlockQueueId = 0)
    {
        var attempt = rangeBlock.Attempt + 1;
        var createRequest = new BlockExecutionCreateRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            rangeBlock.RangeBlockId,
            attempt);

        var blockExecutionId = await _blockRepository.AddRangeBlockExecutionAsync(createRequest).ConfigureAwait(false);
        var blockContext = new RangeBlockContext(_rangeBlockRepository,
            _taskExecutionRepository,
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            rangeBlock, blockExecutionId, _serviceProvider, _loggerFactory, _retryService,
            forcedBlockQueueId);

        return blockContext;
    }

    private async Task<List<RangeBlockContext>> GenerateNewDateRangeBlocksAsync(DateRangeBlockRequest blockRequest,
        int blockCountLimit)
    {
        var newBlocks = new List<RangeBlockContext>();
        var blockStart = blockRequest.RangeBegin.Value;
        var blockEnd = blockStart.Add(blockRequest.MaxBlockRange.Value);
        var blocksAdded = 0;
        var stopGeneration = false;

        while (blocksAdded < blockCountLimit && blockStart <= blockRequest.RangeEnd && stopGeneration == false)
        {
            if (blockEnd >= blockRequest.RangeEnd.Value)
            {
                blockEnd = blockRequest.RangeEnd.Value;
                stopGeneration = true;
            }

            var dateRangeBlock =
                await GenerateDateRangeBlockAsync(blockRequest, blockStart, blockEnd).ConfigureAwait(false);
            var blockContext = await CreateBlockContextAsync(blockRequest, dateRangeBlock).ConfigureAwait(false);
            newBlocks.Add(blockContext);
            blocksAdded++;

            blockStart = blockStart.Add(blockRequest.MaxBlockRange.Value);
            blockEnd = blockStart.Add(blockRequest.MaxBlockRange.Value);
        }

        return newBlocks;
    }

    private async Task<RangeBlock> GenerateDateRangeBlockAsync(DateRangeBlockRequest blockRequest, DateTime rangeBegin,
        DateTime rangeEnd)
    {
        var request = new RangeBlockCreateRequest(blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            rangeBegin,
            rangeEnd);

        var rangeBlock = (await _blockRepository.AddRangeBlockAsync(request).ConfigureAwait(false)).Block;
        await Task.Delay(10).ConfigureAwait(false); // guarantee that each block has a unique created date
        return rangeBlock;
    }

    private async Task<List<RangeBlockContext>> GenerateNewNumericRangeBlocksAsync(
        NumericRangeBlockRequest blockRequest, int blockCountLimit)
    {
        var newBlocks = new List<RangeBlockContext>();
        var blockStart = blockRequest.RangeBegin.Value;
        var blockEnd = blockStart + (blockRequest.BlockSize.Value - 1);
        var blocksAdded = 0;
        var stopGeneration = false;

        while (blocksAdded < blockCountLimit && blockStart <= blockRequest.RangeEnd.Value && stopGeneration == false)
        {
            if (blockEnd >= blockRequest.RangeEnd.Value)
            {
                blockEnd = blockRequest.RangeEnd.Value;
                stopGeneration = true;
            }

            var numericRangeBlock = await GenerateNumericRangeBlockAsync(blockRequest, blockStart, blockEnd)
                .ConfigureAwait(false);
            var blockContext = await CreateBlockContextAsync(blockRequest, numericRangeBlock).ConfigureAwait(false);
            newBlocks.Add(blockContext);
            blocksAdded++;

            blockStart = blockStart + blockRequest.BlockSize.Value;
            blockEnd = blockStart + (blockRequest.BlockSize.Value - 1);
        }

        return newBlocks;
    }

    private async Task<RangeBlock> GenerateNumericRangeBlockAsync(NumericRangeBlockRequest blockRequest,
        long rangeBegin, long rangeEnd)
    {
        var request = new RangeBlockCreateRequest(blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            rangeBegin,
            rangeEnd);

        var rangeBlock = (await _blockRepository.AddRangeBlockAsync(request).ConfigureAwait(false)).Block;
        await Task.Delay(10).ConfigureAwait(false); // guarantee that each block has a unique created date
        return rangeBlock;
    }


    private async Task<List<ProtoListBlock>> CreateProtoListBlocksAsync(ListBlockRequest blockRequest)
    {
        var blocks = new List<ProtoListBlock>();

        if (blockRequest.ReprocessReferenceValue != Guid.Empty)
        {
            blocks = (await LoadListBlocksOfTaskAsync(blockRequest).ConfigureAwait(false)).ToList();
        }
        else
        {
            // Forced blocks
            var forceBlockQueueItems = await GetForcedListBlocksAsync(blockRequest).ConfigureAwait(false);
            var forceBlocks = new List<ProtoListBlock>();
            foreach (var forceBlockQueueItem in forceBlockQueueItems)
            {
                var forceBlock = forceBlockQueueItem.ListBlock;
                forceBlock.IsForcedBlock = true;
                forceBlock.ForcedBlockQueueId = forceBlockQueueItem.ForcedBlockQueueId;
                forceBlocks.Add(forceBlock);
            }

            blocks.AddRange(forceBlocks);

            // Failed and Dead blocks
            if (GetBlocksRemaining(blockRequest, blocks) > 0)
                await LoadFailedAndDeadListBlocksAsync(blockRequest, blocks).ConfigureAwait(false);

            // New blocks
            var blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
            if (blocksRemaining > 0 && blockRequest.SerializedValues != null && blockRequest.SerializedValues.Any())
                blocks.AddRange(await GenerateNewListBlocksAsync(blockRequest, blocksRemaining).ConfigureAwait(false));
        }

        return blocks;
    }

    private int GetBlocksRemaining(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
    {
        return blockRequest.MaxBlocks - blocks.Count;
    }

    private async Task<IList<ForcedListBlockQueueItem>> GetForcedListBlocksAsync(ListBlockRequest blockRequest)
    {
        var forcedBlockRequest = new QueuedForcedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType);

        var queuedForcedBlocks =
            await _blockRepository.GetQueuedForcedListBlocksAsync(forcedBlockRequest).ConfigureAwait(false);

        return queuedForcedBlocks;
    }

    private async Task LoadFailedAndDeadListBlocksAsync(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
    {
        var blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
        if (blockRequest.ReprocessDeadTasks)
        {
            blocks.AddRange(await GetDeadListBlocksAsync(blockRequest, blocksRemaining).ConfigureAwait(false));
            blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
        }

        if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.ReprocessFailedTasks)
            blocks.AddRange(await GetFailedListBlocksAsync(blockRequest, blocksRemaining).ConfigureAwait(false));
    }

    private async Task<IList<ProtoListBlock>> GetDeadListBlocksAsync(ListBlockRequest blockRequest, int blockCountLimit)
    {
        var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
        var deadBlocks = await _blockRepository.FindDeadListBlocksAsync(deadBlockRequest).ConfigureAwait(false);

        return deadBlocks;
    }

    private async Task<IList<ProtoListBlock>> GetFailedListBlocksAsync(ListBlockRequest blockRequest,
        int blockCountLimit)
    {
        var failedBlockRequest = new FindFailedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
            DateTime.UtcNow,
            blockCountLimit,
            blockRequest.FailedTaskRetryLimit
        );

        var failedBlocks = await _blockRepository.FindFailedListBlocksAsync(failedBlockRequest).ConfigureAwait(false);
        _logger.Debug($"Count={failedBlocks.Count}");
        return failedBlocks;
    }

    private async Task<List<ProtoListBlock>> GenerateNewListBlocksAsync(ListBlockRequest blockRequest,
        int blockCountLimit)
    {
        var newBlocks = new List<ProtoListBlock>();
        var listLength = blockRequest.SerializedValues.Count;
        var listIndex = 0;
        var blocksAdded = 0;

        var values = new List<string>();

        while (blocksAdded < blockCountLimit && listIndex < listLength)
        {
            values.Add(blockRequest.SerializedValues[listIndex]);

            if (values.Count == blockRequest.MaxBlockSize || listIndex == listLength - 1)
            {
                var newListBlock = await GenerateListBlockAsync(blockRequest, values).ConfigureAwait(false);
                newBlocks.Add(newListBlock);
                values = new List<string>();
                blocksAdded++;
            }

            listIndex++;
        }

        return newBlocks;
    }

    private async Task<ProtoListBlock> GenerateListBlockAsync(ListBlockRequest blockRequest, List<string> values)
    {
        var request = new ListBlockCreateRequest(blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            values,
            blockRequest.SerializedHeader,
            blockRequest.CompressionThreshold);

        var listBlock = (await _blockRepository.AddListBlockAsync(request).ConfigureAwait(false)).Block;
        await Task.Delay(10).ConfigureAwait(false); // guarantee that each block has a unique created date
        return listBlock;
    }


    private async Task<IList<IListBlockContext<T>>> CreateListBlockContextsAsync<T>(ListBlockRequest blockRequest,
        IList<ProtoListBlock> listBlocks)
    {
        var blocks = new List<IListBlockContext<T>>();
        foreach (var listBlock in listBlocks)
        {
            var blockContext = await CreateListBlockContextAsync<T>(blockRequest, listBlock).ConfigureAwait(false);
            blocks.Add(blockContext);
        }

        return blocks;
    }

    private async Task<IListBlockContext<T>> CreateListBlockContextAsync<T>(ListBlockRequest blockRequest,
        ProtoListBlock listBlock, int forcedBlockQueueId = 0)
    {
        var attempt = listBlock.Attempt + 1;
        var createRequest = new BlockExecutionCreateRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            BlockType.List,
            listBlock.ListBlockId,
            attempt);

        var blockExecutionId = await _blockRepository.AddListBlockExecutionAsync(createRequest).ConfigureAwait(false);

        var listBlockOfT = Convert<T>(listBlock);
        var blockContext = new ListBlockContext<T>(_listBlockRepository,
            _taskExecutionRepository,
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.ListUpdateMode,
            blockRequest.UncommittedItemsThreshold,
            listBlockOfT, _retryService,
            blockExecutionId,
            blockRequest.MaxStatusReasonLength, _loggerFactory, _serviceProvider,
            forcedBlockQueueId);

        return blockContext;
    }

    private async Task<IList<IListBlockContext<TItem, THeader>>> CreateListBlockContextsAsync<TItem, THeader>(
        ListBlockRequest blockRequest, IList<ProtoListBlock> listBlocks)
    {
        var blocks = new List<IListBlockContext<TItem, THeader>>();
        foreach (var listBlock in listBlocks)
        {
            var blockContext = await CreateListBlockContextAsync<TItem, THeader>(blockRequest, listBlock)
                .ConfigureAwait(false);
            blocks.Add(blockContext);
        }

        return blocks;
    }

    private async Task<IListBlockContext<TItem, THeader>> CreateListBlockContextAsync<TItem, THeader>(
        ListBlockRequest blockRequest, ProtoListBlock listBlock, int forcedBlockQueueId = 0)
    {
        var attempt = listBlock.Attempt + 1;
        var createRequest = new BlockExecutionCreateRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            listBlock.ListBlockId,
            attempt);

        var blockExecutionId = await _blockRepository.AddListBlockExecutionAsync(createRequest).ConfigureAwait(false);

        var listBlockOfT = Convert<TItem, THeader>(listBlock);
        var blockContext = new ListBlockContext<TItem, THeader>(_listBlockRepository,
            _taskExecutionRepository,
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.ListUpdateMode,
            blockRequest.UncommittedItemsThreshold,
            listBlockOfT,
            blockExecutionId,
            blockRequest.MaxStatusReasonLength, _serviceProvider, _loggerFactory, _retryService,
            forcedBlockQueueId);

        return blockContext;
    }

    private ListBlock<T> Convert<T>(ProtoListBlock protoListBlock, bool fillBlock = false)
    {
        if (protoListBlock == null)
            return null;

        var block = new ListBlock<T>(_loggerFactory.CreateLogger<ListBlock<T>>());
        block.Attempt = protoListBlock.Attempt;

        if (fillBlock)
            block.Items = Convert<T>(protoListBlock.Items);

        block.ListBlockId = protoListBlock.ListBlockId;

        return block;
    }

    private ListBlock<TItem, THeader> Convert<TItem, THeader>(ProtoListBlock protoListBlock, bool fillBlock = false)
    {
        if (protoListBlock == null)
            return null;

        var block = new ListBlock<TItem, THeader>(_serviceProvider
            .CreateLogger<ListBlock<TItem, THeader>>());
        block.Attempt = protoListBlock.Attempt;

        if (fillBlock)
            block.Items = Convert<TItem>(protoListBlock.Items);

        block.ListBlockId = protoListBlock.ListBlockId;
        block.Header = JsonGenericSerializer.Deserialize<THeader>(protoListBlock.Header);

        return block;
    }

    private IList<IListBlockItem<T>> Convert<T>(IList<ProtoListBlockItem> protoListBlockItems)
    {
        if (protoListBlockItems == null)
            return null;

        var items = new List<IListBlockItem<T>>();

        foreach (var protoItem in protoListBlockItems)
        {
            var item = new ListBlockItem<T>(_loggerFactory.CreateLogger<ListBlockItem<T>>());
            item.LastUpdated = protoItem.LastUpdated;
            item.ListBlockItemId = protoItem.ListBlockItemId;
            item.Status = protoItem.Status;
            item.StatusReason = protoItem.StatusReason;
            item.Step = protoItem.Step;
            item.Value = JsonGenericSerializer.Deserialize<T>(protoItem.Value);

            items.Add(item);
        }

        return items;
    }


    private async Task<List<IObjectBlockContext<T>>> LoadObjectBlocksOfTaskAsync<T>(ObjectBlockRequest<T> blockRequest)
    {
        var failedBlockRequest = new FindBlocksOfTaskRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            blockRequest.ReprocessReferenceValue,
            blockRequest.ReprocessOption);

        var blocksOfTask =
            await _blockRepository.FindObjectBlocksOfTaskAsync<T>(failedBlockRequest).ConfigureAwait(false);
        if (!blocksOfTask.Any())
            return new List<IObjectBlockContext<T>>();

        return await CreateObjectBlockContextsAsync(blockRequest, blocksOfTask).ConfigureAwait(false);
    }

    private async Task<List<IObjectBlockContext<T>>> GetForcedObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest)
    {
        var forcedBlockRequest = new QueuedForcedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType);

        var queuedForcedBlocks = await _blockRepository.GetQueuedForcedObjectBlocksAsync<T>(forcedBlockRequest)
            .ConfigureAwait(false);

        var forcedBlocks = new List<IObjectBlockContext<T>>();
        foreach (var queuedForcedBlock in queuedForcedBlocks)
            forcedBlocks.Add(await CreateObjectBlockContextAsync(blockRequest, queuedForcedBlock.ObjectBlock,
                queuedForcedBlock.ForcedBlockQueueId).ConfigureAwait(false));

        if (forcedBlocks.Any())
            await DequeueForcedBlocksAsync(blockRequest, forcedBlocks.Select(x => x.ForcedBlockQueueId).ToList())
                .ConfigureAwait(false);

        return forcedBlocks;
    }

    private async Task LoadFailedAndDeadObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest,
        List<IObjectBlockContext<T>> blocks)
    {
        var blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
        if (blockRequest.ReprocessDeadTasks)
        {
            blocks.AddRange(await GetDeadObjectBlocksAsync(blockRequest, blocksRemaining).ConfigureAwait(false));
            blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
        }

        if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.ReprocessFailedTasks)
            blocks.AddRange(await GetFailedObjectBlocksAsync(blockRequest, blocksRemaining).ConfigureAwait(false));
    }

    private async Task<List<IObjectBlockContext<T>>> GetDeadObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest,
        int blockCountLimit)
    {
        var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
        var deadBlocks = await _blockRepository.FindDeadObjectBlocksAsync<T>(deadBlockRequest).ConfigureAwait(false);
        return await CreateObjectBlockContextsAsync(blockRequest, deadBlocks).ConfigureAwait(false);
    }

    private async Task<List<IObjectBlockContext<T>>> GetFailedObjectBlocksAsync<T>(ObjectBlockRequest<T> blockRequest,
        int blockCountLimit)
    {
        var failedBlockRequest = new FindFailedBlocksRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
            DateTime.UtcNow,
            blockCountLimit,
            blockRequest.FailedTaskRetryLimit
        );

        var failedBlocks =
            await _blockRepository.FindFailedObjectBlocksAsync<T>(failedBlockRequest).ConfigureAwait(false);
        return await CreateObjectBlockContextsAsync(blockRequest, failedBlocks).ConfigureAwait(false);
    }

    private async Task<List<IObjectBlockContext<T>>> CreateObjectBlockContextsAsync<T>(
        ObjectBlockRequest<T> blockRequest, IList<ObjectBlock<T>> objectBlocks)
    {
        var blocks = new List<IObjectBlockContext<T>>();
        foreach (var objectBlock in objectBlocks)
        {
            var blockContext = await CreateObjectBlockContextAsync(blockRequest, objectBlock).ConfigureAwait(false);
            blocks.Add(blockContext);
        }

        return blocks;
    }

    private async Task<IObjectBlockContext<T>> CreateObjectBlockContextAsync<T>(ObjectBlockRequest<T> blockRequest,
        ObjectBlock<T> objectBlock, int forcedBlockQueueId = 0)
    {
        var attempt = objectBlock.Attempt + 1;
        var createRequest = new BlockExecutionCreateRequest(
            blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.BlockType,
            objectBlock.ObjectBlockId,
            attempt);

        var blockExecutionId = await _blockRepository.AddObjectBlockExecutionAsync(createRequest).ConfigureAwait(false);
        var blockContext = new ObjectBlockContext<T>(_loggerFactory,
            _objectBlockRepository, _retryService, _taskExecutionRepository, blockRequest.TaskId,
            blockRequest.TaskExecutionId, objectBlock, blockExecutionId, forcedBlockQueueId);

        return blockContext;
    }

    private async Task<IObjectBlockContext<T>> GenerateNewObjectBlockAsync<T>(ObjectBlockRequest<T> blockRequest)
    {
        var newObjectBlock = await GenerateObjectBlockAsync(blockRequest).ConfigureAwait(false);
        return await CreateObjectBlockContextAsync(blockRequest, newObjectBlock).ConfigureAwait(false);
    }

    private async Task<ObjectBlock<T>> GenerateObjectBlockAsync<T>(ObjectBlockRequest<T> blockRequest)
    {
        var request = new ObjectBlockCreateRequest<T>(blockRequest.TaskId,
            blockRequest.TaskExecutionId,
            blockRequest.Object,
            blockRequest.CompressionThreshold);

        var objectBlock = (await _blockRepository.AddObjectBlockAsync(request).ConfigureAwait(false)).Block;
        await Task.Delay(10).ConfigureAwait(false); // guarantee that each block has a unique created date
        return objectBlock;
    }
}