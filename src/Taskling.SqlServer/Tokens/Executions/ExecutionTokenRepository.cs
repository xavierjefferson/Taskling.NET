using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Extensions;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;
using TaskDefinition = Taskling.SqlServer.Models.TaskDefinition;

namespace Taskling.SqlServer.Tokens.Executions;

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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _commonTokenRepository = commonTokenRepository;
        _executionTokenHelper = executionTokenHelper;
    }

    public async Task<TokenResponse> TryAcquireExecutionTokenAsync(TokenRequest tokenRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("35264551-c92b-449f-8c69-aa0ec7565e1a");
        _logger.LogDebug($"Acquiring row lock {taskDefinitionId} {taskExecutionId}");
        await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext)
            .ConfigureAwait(false);
    }

    private async Task<ExecutionTokenList> GetTokensAsync(long taskDefinitionId,
        TasklingDbContext dbContext)
    {
        _logger.Debug("57527216-a481-4f7f-9700-05bb9961e403");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var tokensString = await GetTokensStringAsync(taskDefinitionId, dbContext).ConfigureAwait(false);
        _logger.LogDebug($"Retrieved token string '{tokensString}'");
        return ParseTokensString(tokensString);
    }

    public ExecutionTokenList ParseTokensString(string tokensString)
    {
        ExecutionTokenList result = null;
        try
        {
            _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
            _logger.Debug("5f5e58bf-68ce-42da-a855-2324e0260171");
            _logger.Debug($"Retrieved tokens {Constants.Serialize(result)}");
        }
    }

    private bool AdjustTokenCount(ExecutionTokenList tokenList, int concurrencyCount)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var modified = false;

        if (concurrencyCount == -1 || concurrencyCount == 0) // if there is no limit
        {
            _logger.Debug("ec397e1a-e44d-4906-8018-e4cf12bd191e");
            if (tokenList.Count != 1 || (tokenList.Count == 1 &&
                                         tokenList.All(x => x.Status != ExecutionTokenStatus.Unlimited)))
            {
                _logger.Debug("58087c5f-eed6-421f-80ce-fbb6ff039b4c");
                tokenList.Clear();
                tokenList.Add(_executionTokenHelper.Create(ExecutionTokenStatus.Unlimited));

                modified = true;
            }
        }
        else
        {
            _logger.Debug("cb557d6b-f621-48e4-852c-0d66d8988914");
            // if has a limit then remove any unlimited tokens
            if (tokenList.Any(i => i.Status == ExecutionTokenStatus.Unlimited))
            {
                tokenList.RemoveAll(i => i.Status == ExecutionTokenStatus.Unlimited);
                modified = true;
            }

            // the current token count is less than the limit then add new tokens
            if (tokenList.Count < concurrencyCount)
            {
                _logger.Debug("bf7e17e0-5e62-481f-84b8-e2eabc6b70f6");
                while (tokenList.Count < concurrencyCount)
                {
                    tokenList.Add(_executionTokenHelper.Create(ExecutionTokenStatus.Available));

                    modified = true;
                }
            }
            // if the current token count is greater than the limit then
            // start removing tokens. Remove Available tokens preferentially.
            else if (tokenList.Count > concurrencyCount)
            {
                _logger.Debug("9f8c6054-d234-4385-85fe-40c8eccef8dd");
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
        }

        return modified;
    }

    private ExecutionTokenList ReturnDefaultTokenList()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var list = new ExecutionTokenList
        {
            _executionTokenHelper.Create(ExecutionTokenStatus.Available)
        };

        return list;
    }

    private async Task<string> GetTokensStringAsync(long taskDefinitionId,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("e16ddbfb-e560-488c-bfab-6ed074491742");
        _logger.Debug($"Fetching token string {taskDefinitionId}");
        var tokens = await dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
            .Select(i => i.ExecutionTokens)
            .FirstOrDefaultAsync().ConfigureAwait(false);
        if (tokens != null)
        {
            _logger.Debug("1a105fe2-3e3b-4c27-adeb-08f9222b2dd5");
            return tokens;
        }

        _logger.Debug("5c1db988-f600-4653-af45-9b383dd3cedf");
        return string.Empty;
    }

    private async Task<ExecutionToken?> GetAssignableTokenAsync(ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("ad87e743-7aba-4535-8cbe-23262e2cd6aa");
        if (HasAvailableToken(executionTokenList))
        {
            _logger.Debug("a104f62b-4c02-4ccc-a2c0-002b88b5b8ff");
            return GetAvailableToken(executionTokenList);
        }

        var executionIds = executionTokenList
            .Where(x => x.Status != ExecutionTokenStatus.Disabled && x.GrantedToExecution != 0)
            .Select(x => x.GrantedToExecution)
            .ToList();

        if (!executionIds.Any())
        {
            _logger.Debug("95c852d3-a7a5-464a-bf17-69483e5c22b9");
            return null;
        }

        var executionStates = await GetTaskExecutionStatesAsync(executionIds, dbContext).ConfigureAwait(false);
        var expiredExecution = FindExpiredExecution(executionStates);
        if (expiredExecution == null)
        {
            _logger.Debug("da006c69-4899-4c07-acbf-90e0abf6ac71");
            return null;
        }

        return executionTokenList.First(x => x.GrantedToExecution == expiredExecution.TaskExecutionId);
    }

    private bool HasAvailableToken(ExecutionTokenList executionTokenList)
    {
        _logger.Debug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("07545a25-612f-463d-93a8-7439a6871014");
        var tmp = executionTokenList.Any(x => x.Status == ExecutionTokenStatus.Available
                                              || x.Status == ExecutionTokenStatus.Unlimited);
        _logger.Debug($"{Constants.Serialize(executionTokenList)}");
        _logger.Debug($"{nameof(HasAvailableToken)}={tmp}");
        return tmp;
    }

    private ExecutionToken GetAvailableToken(ExecutionTokenList executionTokenList)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var tmp = executionTokenList.FirstOrDefault(x => x.Status == ExecutionTokenStatus.Available
                                                         || x.Status == ExecutionTokenStatus.Unlimited);
        _logger.LogDebug($"Available token = {(tmp == null ? "null" : tmp.TokenId.ToString())}");
        return tmp;
    }

    private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, dbContext)
            .ConfigureAwait(false);
    }

    private TaskExecutionState? FindExpiredExecution(List<TaskExecutionState> executionStates)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        foreach (var teState in executionStates)
            if (HasExpired(teState))
            {
                _logger.Debug("645a2465-0b33-4a0d-9c04-6355f0f2b93c");
                return teState;
            }

        _logger.Debug("8ecc12fb-3fe6-4b77-81e6-2f918c32dd53");
        return null;
    }

    private bool HasExpired(TaskExecutionState taskExecutionState)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _commonTokenRepository.HasExpired(taskExecutionState);
    }

    private void AssignToken(ExecutionToken executionToken, long taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        _executionTokenHelper.SetGrantedToExecution(executionToken, taskExecutionId);
        if (executionToken.Status != ExecutionTokenStatus.Unlimited)
        {
            _logger.Debug("904ba3e8-cbc2-4c81-beb3-ec573422d7b2");
            _executionTokenHelper.SetStatus(executionToken, ExecutionTokenStatus.Unavailable);
        }
    }

    private async Task PersistTokensAsync(long taskDefinitionId, ExecutionTokenList executionTokenList,
        TasklingDbContext dbContext)
    {
        _logger.Debug("83dc1ee9-0eb9-45a8-b306-77636f795ab1");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var tokenString = executionTokenList.Serialize();
        _logger.Debug($"Persisting token {taskDefinitionId} {tokenString}");
        var taskDefinition = new TaskDefinition { TaskDefinitionId = taskDefinitionId, ExecutionTokens = tokenString };
        var entityEntry = dbContext.TaskDefinitions.Attach(taskDefinition);
        entityEntry.Property(i => i.ExecutionTokens).IsModified = true;
        await dbContext.SaveChangesAsync();
        entityEntry.State = EntityState.Detached;
    }

    private void SetTokenAsAvailable(ExecutionTokenList executionTokenList, Guid executionTokenId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var executionToken = executionTokenList.FirstOrDefault(x => x.TokenId == executionTokenId);
        if (executionToken != null && executionToken.Status == ExecutionTokenStatus.Unavailable)
        {
            _logger.Debug("01e0d5ac-c41b-40ce-8fe1-61d434ee1284");
            executionToken.Status = ExecutionTokenStatus.Available;
        }
        else
        {
            _logger.Debug("066ab17a-c65b-42cc-8cb5-6f250a912776");
        }
    }
}