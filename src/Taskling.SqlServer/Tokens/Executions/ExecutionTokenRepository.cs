using System.Text;
using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionTokenRepository : DbOperationsService, IExecutionTokenRepository
{
    private readonly ICommonTokenRepository _commonTokenRepository;

    public ExecutionTokenRepository(ICommonTokenRepository commonTokenRepository, IConnectionStore connectionStore,
        IDbContextFactoryEx dbContextFactoryEx) : base(connectionStore, dbContextFactoryEx)
    {
        _commonTokenRepository = commonTokenRepository;
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


    private async Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId,
        TasklingDbContext dbContext)
    {
        await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext)
            .ConfigureAwait(false);
    }

    private async Task<ExecutionTokenList> GetTokensAsync(int taskDefinitionId,
        TasklingDbContext dbContext)
    {
        var tokensString = await GetTokensStringAsync(taskDefinitionId, dbContext).ConfigureAwait(false);
        return ParseTokensString(tokensString);
    }

    public static ExecutionTokenList ParseTokensString(string tokensString)
    {
        if (string.IsNullOrWhiteSpace(tokensString))
            return ReturnDefaultTokenList();

        var tokenList = ExecutionTokenList.Deserialize(tokensString);

        return tokenList;
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
                tokenList.Add(new ExecutionToken
                {
                    TokenId = Guid.NewGuid(),
                    Status = ExecutionTokenStatus.Unlimited,
                    GrantedToExecution = 0
                });

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
                    tokenList.Add(new ExecutionToken
                    {
                        TokenId = Guid.NewGuid(),
                        Status = ExecutionTokenStatus.Available,
                        GrantedToExecution = 0
                    });

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

    private static ExecutionTokenList ReturnDefaultTokenList()
    {
        var list = new ExecutionTokenList
        {
            new ExecutionToken
            {
                TokenId = Guid.NewGuid(),
                Status = ExecutionTokenStatus.Available
            }
        };

        return list;
    }

    private async Task<string> GetTokensStringAsync(int taskDefinitionId,
        TasklingDbContext dbContext)
    {
        var tokens = await dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
            .Select(i => i.ExecutionTokens)
            .FirstOrDefaultAsync().ConfigureAwait(false);
        return tokens ?? string.Empty;
    }

    private async Task<ExecutionToken?> GetAssignableTokenAsync(ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        if (HasAvailableToken(executionTokenList)) return GetAvailableToken(executionTokenList);

        var executionIds = executionTokenList
            .Where(x => x.Status != ExecutionTokenStatus.Disabled && x.GrantedToExecution != 0)
            .Select(x => x.GrantedToExecution)
            .ToList();

        if (!executionIds.Any())
            return null;

        var executionStates = await GetTaskExecutionStatesAsync(executionIds, dbContext).ConfigureAwait(false);
        var expiredExecution = FindExpiredExecution(executionStates);
        if (expiredExecution == null)
            return null;

        return executionTokenList.First(x => x.GrantedToExecution == expiredExecution.TaskExecutionId);
    }

    private bool HasAvailableToken(ExecutionTokenList executionTokenList)
    {
        return executionTokenList.Any(x => x.Status == ExecutionTokenStatus.Available
                                           || x.Status == ExecutionTokenStatus.Unlimited);
    }

    private ExecutionToken GetAvailableToken(ExecutionTokenList executionTokenList)
    {
        return executionTokenList.FirstOrDefault(x => x.Status == ExecutionTokenStatus.Available
                                                      || x.Status == ExecutionTokenStatus.Unlimited);
    }

    private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
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

    private void AssignToken(ExecutionToken executionToken, int taskExecutionId)
    {
        executionToken.GrantedToExecution = taskExecutionId;

        if (executionToken.Status != ExecutionTokenStatus.Unlimited)
            executionToken.Status = ExecutionTokenStatus.Unavailable;
    }

    private async Task PersistTokensAsync(int taskDefinitionId, ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        var tokenString = executionTokenList.Serialize();
        var taskDefinitions = await
            dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId).ToListAsync();
        foreach (var taskDefinition in taskDefinitions)
        {
            taskDefinition.ExecutionTokens = tokenString;
            dbContext.TaskDefinitions.Update(taskDefinition);
        }

        await dbContext.SaveChangesAsync().ConfigureAwait(false);
    }

    private void SetTokenAsAvailable(ExecutionTokenList executionTokenList, Guid executionTokenId)
    {
        var executionToken = executionTokenList.FirstOrDefault(x => x.TokenId == executionTokenId);
        if (executionToken != null && executionToken.Status == ExecutionTokenStatus.Unavailable)
            executionToken.Status = ExecutionTokenStatus.Available;
    }
}