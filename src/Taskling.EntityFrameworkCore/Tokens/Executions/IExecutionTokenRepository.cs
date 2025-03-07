﻿namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public interface IExecutionTokenRepository
{
    Task<TokenResponse> TryAcquireExecutionTokenAsync(TokenRequest tokenRequest);
    Task ReturnExecutionTokenAsync(TokenRequest tokenRequest, Guid executionTokenId);
}