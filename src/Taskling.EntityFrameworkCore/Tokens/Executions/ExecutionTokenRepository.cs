using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Extensions;
using Taskling.InfrastructureContracts.TaskExecution;
using TaskDefinition = Taskling.EntityFrameworkCore.Models.TaskDefinition;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class ExecutionTokenRepository : DbOperationsService, IExecutionTokenRepository
{
    private readonly ICommonTokenRepository _commonTokenRepository;
    private readonly IExecutionTokenHelper _executionTokenHelper;
    private readonly ILogger<ExecutionTokenRepository> _logger;

    public ExecutionTokenRepository(ICommonTokenRepository commonTokenRepository, IConnectionStore connectionStore,
        IExecutionTokenHelper executionTokenHelper,
        IDbContextFactoryEx dbContextFactoryEx, ILogger<ExecutionTokenRepository> logger, ILoggerFactory loggerFactory)
        : base(connectionStore,
            dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _logger = logger;
        _commonTokenRepository = commonTokenRepository;
        _executionTokenHelper = executionTokenHelper;
    }

    public async Task<TokenResponse> TryAcquireExecutionTokenAsync(TokenRequest tokenRequest)
    {
        if (tokenRequest == null) throw new ArgumentNullException(nameof(tokenRequest));
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var response = new TokenResponse
            {
                StartedAt = DateTime.UtcNow
            };

            using (var dbContext = await GetDbContextAsync(tokenRequest.TaskId))
            {
                await AcquireRowLockAsync(tokenRequest.TaskDefinitionId, tokenRequest.TaskExecutionId,
                        dbContext)
                    .ConfigureAwait(false);
                var tokens = await GetTokensAsync(tokenRequest.TaskDefinitionId, dbContext)
                    .ConfigureAwait(false);
                var adjusted = AdjustTokenCount(tokens, tokenRequest.ConcurrencyLimit);
                var assignableToken = await GetAssignableTokenAsync(tokens, dbContext).ConfigureAwait(false);
                if (assignableToken == null)
                {
                    response.GrantStatus = GrantStatus.Denied;
                    response.ExecutionTokenId = Guid.Empty;
                }
                else
                {
                    AssignToken(assignableToken, tokenRequest.TaskExecutionId);
                    response.GrantStatus = GrantStatus.Granted;
                    response.ExecutionTokenId = assignableToken.TokenId;
                    adjusted = true;
                }

                if (adjusted)
                    await PersistTokensAsync(tokenRequest.TaskDefinitionId, tokens, dbContext).ConfigureAwait(false);


                return response;
            }
        });
    }

    public async Task ReturnExecutionTokenAsync(TokenRequest tokenRequest, Guid executionTokenId)
    {
        if (tokenRequest == null) throw new ArgumentNullException(nameof(tokenRequest));
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(tokenRequest.TaskId).ConfigureAwait(false))
            {
                await AcquireRowLockAsync(tokenRequest.TaskDefinitionId, tokenRequest.TaskExecutionId,
                        dbContext)
                    .ConfigureAwait(false);
                var tokens = await GetTokensAsync(tokenRequest.TaskDefinitionId, dbContext)
                    .ConfigureAwait(false);
                SetTokenAsAvailable(tokens, executionTokenId);
                await PersistTokensAsync(tokenRequest.TaskDefinitionId, tokens, dbContext).ConfigureAwait(false);
            }
        }, 10, 60000);
    }


    private async Task AcquireRowLockAsync(long taskDefinitionId, long taskExecutionId,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug($"Acquiring row lock {taskDefinitionId} {taskExecutionId}");
        await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext)
            .ConfigureAwait(false);
    }

    private async Task<ExecutionTokenList> GetTokensAsync(long taskDefinitionId,
        TasklingDbContext dbContext)
    {
        var tokensString = await GetTokensStringAsync(taskDefinitionId, dbContext).ConfigureAwait(false);
        _logger.LogDebug($"Retrieved token string '{tokensString}'");
        return ParseTokensString(tokensString);
    }

    public ExecutionTokenList ParseTokensString(string tokensString)
    {
        ExecutionTokenList result = null;
        try
        {
            if (string.IsNullOrWhiteSpace(tokensString))
            {
                result = ReturnDefaultTokenList();
            }
            else
            {
                var tokenList = ExecutionTokenList.Deserialize(tokensString);

                result = tokenList;
            }

            return result;
        }
        finally
        {
            _logger.LogDebug($"Retrieved tokens {Constants.Serialize(result)}");
        }
    }

    private bool AdjustTokenCount(ExecutionTokenList tokenList, int concurrencyCount)
    {
        var modified = false;

        if (concurrencyCount == -1 || concurrencyCount == 0) // if there is no limit
        {
            if (tokenList.Count != 1 || (tokenList.Count == 1 &&
                                         tokenList.All(x => x.Status != ExecutionTokenStatus.Unlimited)))
            {
                tokenList.Clear();
                tokenList.Add(_executionTokenHelper.Create(ExecutionTokenStatus.Unlimited));

                modified = true;
            }
        }
        else
        {
            // if has a limit then remove any unlimited tokens
            if (tokenList.Any(i => i.Status == ExecutionTokenStatus.Unlimited))
            {
                tokenList.RemoveAll(i => i.Status == ExecutionTokenStatus.Unlimited);
                modified = true;
            }

            // the current token count is less than the limit then add new tokens
            if (tokenList.Count < concurrencyCount)
                while (tokenList.Count < concurrencyCount)
                {
                    tokenList.Add(_executionTokenHelper.Create(ExecutionTokenStatus.Available));

                    modified = true;
                }
            // if the current token count is greater than the limit then
            // start removing tokens. Remove Available tokens preferentially.
            else if (tokenList.Count > concurrencyCount)
                while (tokenList.Count > concurrencyCount)
                {
                    if (tokenList.Any(x => x.Status == ExecutionTokenStatus.Available))
                    {
                        var firstAvailable = tokenList.First(x => x.Status == ExecutionTokenStatus.Available);
                        tokenList.Remove(firstAvailable);
                    }
                    else
                    {
                        tokenList.Remove(tokenList.First());
                    }

                    modified = true;
                }
        }

        return modified;
    }

    private ExecutionTokenList ReturnDefaultTokenList()
    {
        var list = new ExecutionTokenList
        {
            _executionTokenHelper.Create(ExecutionTokenStatus.Available)
        };

        return list;
    }

    private async Task<string> GetTokensStringAsync(long taskDefinitionId,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug($"Fetching token string {taskDefinitionId}");
        var tokens = await dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
            .Select(i => i.ExecutionTokens)
            .FirstOrDefaultAsync().ConfigureAwait(false);
        if (tokens != null) return tokens;

        return string.Empty;
    }

    private async Task<ExecutionToken?> GetAssignableTokenAsync(ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        if (HasAvailableToken(executionTokenList)) return GetAvailableToken(executionTokenList);

        var executionIds = executionTokenList
            .Where(x => x.Status != ExecutionTokenStatus.Disabled && x.GrantedToExecution != 0)
            .Select(x => x.GrantedToExecution)
            .ToList();

        if (!executionIds.Any()) return null;

        var executionStates = await GetTaskExecutionStatesAsync(executionIds, dbContext).ConfigureAwait(false);
        var expiredExecution = FindExpiredExecution(executionStates);
        if (expiredExecution == null) return null;

        return executionTokenList.First(x => x.GrantedToExecution == expiredExecution.TaskExecutionId);
    }

    private bool HasAvailableToken(ExecutionTokenList executionTokenList)
    {
        var tmp = executionTokenList.Any(x => x.Status == ExecutionTokenStatus.Available
                                              || x.Status == ExecutionTokenStatus.Unlimited);
        _logger.LogDebug($"{Constants.Serialize(executionTokenList)}");
        _logger.LogDebug($"{nameof(HasAvailableToken)}={tmp}");
        return tmp;
    }

    private ExecutionToken GetAvailableToken(ExecutionTokenList executionTokenList)
    {
        var tmp = executionTokenList.FirstOrDefault(x => x.Status == ExecutionTokenStatus.Available
                                                         || x.Status == ExecutionTokenStatus.Unlimited);
        _logger.LogDebug($"Available token = {(tmp == null ? "null" : tmp.TokenId.ToString())}");
        return tmp;
    }

    private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext)
    {
        return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, dbContext)
            .ConfigureAwait(false);
    }

    private TaskExecutionState? FindExpiredExecution(List<TaskExecutionState> executionStates)
    {
        foreach (var teState in executionStates)
            if (HasExpired(teState))
                return teState;

        return null;
    }

    private bool HasExpired(TaskExecutionState taskExecutionState)
    {
        return _commonTokenRepository.HasExpired(taskExecutionState);
    }

    private void AssignToken(ExecutionToken executionToken, long taskExecutionId)
    {
        _executionTokenHelper.SetGrantedToExecution(executionToken, taskExecutionId);
        if (executionToken.Status != ExecutionTokenStatus.Unlimited)
            _executionTokenHelper.SetStatus(executionToken, ExecutionTokenStatus.Unavailable);
    }

    private async Task PersistTokensAsync(long taskDefinitionId, ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        var tokenString = executionTokenList.Serialize();
        _logger.LogDebug($"Persisting token {taskDefinitionId} {tokenString}");
        var taskDefinition = new TaskDefinition { TaskDefinitionId = taskDefinitionId, ExecutionTokens = tokenString };
        var entityEntry = dbContext.TaskDefinitions.Attach(taskDefinition);
        entityEntry.Property(i => i.ExecutionTokens).IsModified = true;
        await dbContext.SaveChangesAsync();
        entityEntry.State = EntityState.Detached;
    }

    private void SetTokenAsAvailable(ExecutionTokenList executionTokenList, Guid executionTokenId)
    {
        var executionToken = executionTokenList.FirstOrDefault(x => x.TokenId == executionTokenId);
        if (executionToken != null && executionToken.Status == ExecutionTokenStatus.Unavailable)
        {
            executionToken.Status = ExecutionTokenStatus.Available;
        }
    }
}