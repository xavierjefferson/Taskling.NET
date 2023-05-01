using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.StressTest;

public class TasklingStressTest
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;

    private readonly List<string> _processes = new()
    {
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
        "W", "X", "Y", "Z"
    };

    private readonly Random _random = new();

    public TasklingStressTest(IExecutionsHelper executionsHelper, IClientHelper clientHelper)
    {
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;
    }

    [Fact]
    [Trait("Speed", "Slow")]
    public void StartStressTest()
    {
        CreateTasksAndExecutionTokens();
        var tasks = new List<Task>();
        for (var i = 0; i < 10; i++)
            tasks.Add(Task.Factory.StartNew(async () => await RunRandomTaskAsync(20)));

        Task.WaitAll(tasks.ToArray());
    }

    private void CreateTasksAndExecutionTokens()
    {
        _executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        foreach (var process in _processes)
        {
            var drSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "DR_" + process);
            var nrSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "NR_" + process);
            var lsucSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "LSUC_" + process);
            var lpcSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "LPC_" + process);
            var lbcSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "LBC_" + process);

            var ovdrSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "OV_DR_" + process);
            var ovnrSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "OV_NR_" + process);
            var ovlsucSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "OV_LSUC_" + process);
            var ovlpcSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "OV_LPC_" + process);
            var ovlbcSecondaryId = _executionsHelper.InsertTask(TestConstants.ApplicationName, "OV_LBC_" + process);

            _executionsHelper.InsertAvailableExecutionToken(drSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(nrSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(lsucSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(lpcSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(lbcSecondaryId);

            _executionsHelper.InsertAvailableExecutionToken(ovdrSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(ovnrSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(ovlsucSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(ovlpcSecondaryId);
            _executionsHelper.InsertAvailableExecutionToken(ovlbcSecondaryId);
        }
    }

    private async Task RunRandomTaskAsync(int repeatCount)
    {
        for (var i = 0; i < repeatCount; i++)
        {
            var num = _random.Next(9);
            switch (num)
            {
                case 0:
                    await RunKeepAliveDateRangeTaskAsync();
                    break;
                case 1:
                    await RunKeepAliveNumericRangeTaskAsync();
                    break;
                case 2:
                    await RunKeepAliveListTaskWithSingleUnitCommitAsync();
                    break;
                case 3:
                    await RunKeepAliveListTaskWithPeriodicCommitAsync();
                    break;
                case 4:
                    await RunKeepAliveListTaskWithBatchCommitAsync();
                    break;
                case 5:
                    await RunOverrideDateRangeTaskAsync();
                    break;
                case 6:
                    await RunOverrideNumericRangeTaskAsync();
                    break;
                case 7:
                    await RunOverrideListTaskWithSingleUnitCommitAsync();
                    break;
                case 8:
                    await RunOverrideListTaskWithPeriodicCommitAsync();
                    break;
                case 9:
                    await RunOverrideListTaskWithBatchCommitAsync();
                    break;
            }
        }
    }

    private async Task RunKeepAliveDateRangeTaskAsync()
    {
        var taskName = "DR_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var blocks =
                    await executionContext.GetDateRangeBlocksAsync(
                        x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                            .OverrideConfiguration()
                            .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                            .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                            .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunKeepAliveNumericRangeTaskAsync()
    {
        var taskName = "NR_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 10000, 100)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunKeepAliveListTaskWithSingleUnitCommitAsync()
    {
        var taskName = "LSUC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("SUC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithSingleUnitCommit(values, 50)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunKeepAliveListTaskWithPeriodicCommitAsync()
    {
        var taskName = "LPC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("PC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithPeriodicCommit(values, 50, BatchSize.Hundred)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunKeepAliveListTaskWithBatchCommitAsync()
    {
        var taskName = "LBC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("BC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithBatchCommitAtEnd(values, 50)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }


    private async Task RunOverrideDateRangeTaskAsync()
    {
        var taskName = "OV_DR_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var blocks =
                    await executionContext.GetDateRangeBlocksAsync(
                        x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                            .OverrideConfiguration()
                            .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                            .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                            .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunOverrideNumericRangeTaskAsync()
    {
        var taskName = "OV_NR_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 10000, 100)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunOverrideListTaskWithSingleUnitCommitAsync()
    {
        var taskName = "OV_LSUC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("SUC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithSingleUnitCommit(values, 50)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunOverrideListTaskWithPeriodicCommitAsync()
    {
        var taskName = "OV_LPC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("PC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithPeriodicCommit(values, 50, BatchSize.Hundred)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private async Task RunOverrideListTaskWithBatchCommitAsync()
    {
        var taskName = "OV_LBC_" + _processes[_random.Next(25)];
        Console.WriteLine(taskName);
        using (var executionContext = _clientHelper.GetExecutionContext(taskName,
                   _clientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                using (var cs = executionContext.CreateCriticalSection())
                {
                    await cs.TryStartAsync();
                }

                var values = GetList("BC", 1000);
                var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                    .WithBatchCommitAtEnd(values, 50)
                    .OverrideConfiguration()
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.CompleteAsync();
                }
            }
        }
    }

    private List<PersonDto> GetList(string prefix, int count)
    {
        var list = new List<PersonDto>();

        for (var i = 0; i < count; i++)
            list.Add(new PersonDto { DateOfBirth = DateTime.Now, Id = i, Name = prefix + i });

        return list;
    }
}