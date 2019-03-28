using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Configuration;

namespace Taskling.SqlServer.Tokens.Executions
{
    public class ExecutionTokenRepository : DbOperationsService, IExecutionTokenRepository
    {
        private readonly ICommonTokenRepository _commonTokenRepository;

        public ExecutionTokenRepository(ICommonTokenRepository commonTokenRepository)
        {
            _commonTokenRepository = commonTokenRepository;
        }

        public async Task<TokenResponse> TryAcquireExecutionTokenAsync(TokenRequest tokenRequest)
        {
            var response = new TokenResponse();
            response.StartedAt = DateTime.UtcNow;

            using (var connection = await CreateNewConnectionAsync(tokenRequest.TaskId).ConfigureAwait(false))
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(tokenRequest.TaskId).QueryTimeoutSeconds;

                try
                {
                    await AcquireRowLockAsync(tokenRequest.TaskDefinitionId, tokenRequest.TaskExecutionId, command).ConfigureAwait(false);
                    var tokens = await GetTokensAsync(tokenRequest.TaskDefinitionId, command).ConfigureAwait(false);
                    var adjusted = AdjustTokenCount(tokens, tokenRequest.ConcurrencyLimit);
                    var assignableToken = await GetAssignableTokenAsync(tokens, command).ConfigureAwait(false);
                    if (assignableToken == null)
                    {
                        response.GrantStatus = GrantStatus.Denied;
                        response.ExecutionTokenId = "0";
                    }
                    else
                    {
                        AssignToken(assignableToken, tokenRequest.TaskExecutionId);
                        response.GrantStatus = GrantStatus.Granted;
                        response.ExecutionTokenId = assignableToken.TokenId;
                        adjusted = true;
                    }

                    if (adjusted)
                        await PersistTokensAsync(tokenRequest.TaskDefinitionId, tokens, command).ConfigureAwait(false);

                    transaction.Commit();
                    return response;
                }
                catch (SqlException sqlEx)
                {
                    TryRollBack(transaction, sqlEx);
                    throw;
                }
                catch (Exception ex)
                {
                    TryRollback(transaction, ex);
                    throw;
                }
            }
        }

        public async Task ReturnExecutionTokenAsync(TokenRequest tokenRequest, string executionTokenId)
        {
            using (var connection = await CreateNewConnectionAsync(tokenRequest.TaskId).ConfigureAwait(false))
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(tokenRequest.TaskId).QueryTimeoutSeconds;

                try
                {
                    await AcquireRowLockAsync(tokenRequest.TaskDefinitionId, tokenRequest.TaskExecutionId, command).ConfigureAwait(false);
                    var tokens = await GetTokensAsync(tokenRequest.TaskDefinitionId, command).ConfigureAwait(false);
                    SetTokenAsAvailable(tokens, executionTokenId);
                    await PersistTokensAsync(tokenRequest.TaskDefinitionId, tokens, command).ConfigureAwait(false);

                    transaction.Commit();
                }
                catch (SqlException sqlEx)
                {
                    TryRollBack(transaction, sqlEx);
                }
                catch (Exception ex)
                {
                    TryRollback(transaction, ex);
                }
            }
        }


        private async Task AcquireRowLockAsync(int taskDefinitionId, string taskExecutionId, SqlCommand command)
        {
            await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, command).ConfigureAwait(false);
        }

        private async Task<ExecutionTokenList> GetTokensAsync(int taskDefinitionId, SqlCommand command)
        {
            var tokensString = await GetTokensStringAsync(taskDefinitionId, command).ConfigureAwait(false);
            return ParseTokensString(tokensString);
        }

        public static ExecutionTokenList ParseTokensString(string tokensString)
        {
            if (string.IsNullOrEmpty(tokensString))
                return ReturnDefaultTokenList();

            var tokenList = new ExecutionTokenList();

            var tokens = tokensString.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var tokenText in tokens)
            {
                var token = new ExecutionToken();
                var tokenParts = tokenText.Split(',');
                if (tokenParts.Length != 3)
                    throw new TokenFormatException("Token text not valid. Format is I:<id>,G:<granted TaskExecutionId>,S:<status> Invalid text: " + tokensString);

                foreach (var part in tokenParts)
                {
                    if (part.StartsWith("I:") && part.Length > 2)
                        token.TokenId = part.Substring(2);
                    else if (part.StartsWith("G:") && part.Length > 2)
                        token.GrantedToExecution = part.Substring(2);
                    else if (part.StartsWith("S:") && part.Length > 2)
                        token.Status = (ExecutionTokenStatus)int.Parse(part.Substring(2));
                    else
                        throw new TokenFormatException("Token text not valid. Format is I:<id>,G:<granted TaskExecutionId>,S:<status> Invalid text: " + tokensString);
                }

                tokenList.Tokens.Add(token);
            }

            return tokenList;
        }

        private bool AdjustTokenCount(ExecutionTokenList tokenList, int concurrencyCount)
        {
            bool modified = false;

            if (concurrencyCount == -1 || concurrencyCount == 0) // if there is no limit
            {
                if (tokenList.Tokens.Count != 1 || (tokenList.Tokens.Count == 1 && tokenList.Tokens.All(x => x.Status != ExecutionTokenStatus.Unlimited)))
                {
                    tokenList.Tokens.Clear();
                    tokenList.Tokens.Add(new ExecutionToken()
                    {
                        TokenId = Guid.NewGuid().ToString(),
                        Status = ExecutionTokenStatus.Unlimited,
                        GrantedToExecution = "0"
                    });

                    modified = true;
                }
            }
            else
            {
                // if has a limit then remove any unlimited tokens
                if (tokenList.Tokens.Any(x => x.Status == ExecutionTokenStatus.Unlimited))
                {
                    tokenList.Tokens = tokenList.Tokens.Where(x => x.Status != ExecutionTokenStatus.Unlimited).ToList();
                    modified = true;
                }

                // the current token count is less than the limit then add new tokens
                if (tokenList.Tokens.Count < concurrencyCount)
                {
                    while (tokenList.Tokens.Count < concurrencyCount)
                    {
                        tokenList.Tokens.Add(new ExecutionToken()
                        {
                            TokenId = Guid.NewGuid().ToString(),
                            Status = ExecutionTokenStatus.Available,
                            GrantedToExecution = "0"
                        });

                        modified = true;
                    }
                }
                // if the current token count is greater than the limit then
                // start removing tokens. Remove Available tokens preferentially.
                else if (tokenList.Tokens.Count > concurrencyCount)
                {
                    while (tokenList.Tokens.Count > concurrencyCount)
                    {
                        if (tokenList.Tokens.Any(x => x.Status == ExecutionTokenStatus.Available))
                        {
                            var firstAvailable = tokenList.Tokens.First(x => x.Status == ExecutionTokenStatus.Available);
                            tokenList.Tokens.Remove(firstAvailable);
                        }
                        else
                        {
                            tokenList.Tokens.Remove(tokenList.Tokens.First());
                        }

                        modified = true;
                    }
                }
            }

            return modified;
        }

        private static ExecutionTokenList ReturnDefaultTokenList()
        {
            var list = new ExecutionTokenList();
            list.Tokens.Add(new ExecutionToken()
            {
                TokenId = Guid.NewGuid().ToString(),
                Status = ExecutionTokenStatus.Available
            });

            return list;
        }

        private async Task<string> GetTokensStringAsync(int taskDefinitionId, SqlCommand command)
        {
            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.GetExecutionTokensQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (await reader.ReadAsync().ConfigureAwait(false))
                    return reader[0].ToString();
            }

            return string.Empty;
        }

        private async Task<ExecutionToken> GetAssignableTokenAsync(ExecutionTokenList executionTokenList, SqlCommand command)
        {
            if (HasAvailableToken(executionTokenList))
            {
                return GetAvailableToken(executionTokenList);
            }
            else
            {
                var executionIds = executionTokenList.Tokens.Where(x => x.Status != ExecutionTokenStatus.Disabled && !string.IsNullOrEmpty(x.GrantedToExecution))
                    .Select(x => x.GrantedToExecution)
                    .ToList();

                if (!executionIds.Any())
                    return null;

                var executionStates = await GetTaskExecutionStatesAsync(executionIds, command).ConfigureAwait(false);
                var expiredExecution = FindExpiredExecution(executionStates);
                if (expiredExecution == null)
                    return null;

                return executionTokenList.Tokens.First(x => x.GrantedToExecution == expiredExecution.TaskExecutionId);
            }
        }

        private bool HasAvailableToken(ExecutionTokenList executionTokenList)
        {
            return executionTokenList.Tokens.Any(x => x.Status == ExecutionTokenStatus.Available
                                                                 || x.Status == ExecutionTokenStatus.Unlimited);
        }

        private ExecutionToken GetAvailableToken(ExecutionTokenList executionTokenList)
        {
            return executionTokenList.Tokens.FirstOrDefault(x => x.Status == ExecutionTokenStatus.Available
                                                                 || x.Status == ExecutionTokenStatus.Unlimited);
        }

        private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<string> taskExecutionIds, SqlCommand command)
        {
            return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, command).ConfigureAwait(false);
        }

        private TaskExecutionState FindExpiredExecution(List<TaskExecutionState> executionStates)
        {
            foreach (var teState in executionStates)
            {
                if (HasExpired(teState))
                    return teState;
            }

            return null;
        }

        private bool HasExpired(TaskExecutionState taskExecutionState)
        {
            return _commonTokenRepository.HasExpired(taskExecutionState);
        }

        private void AssignToken(ExecutionToken executionToken, string taskExecutionId)
        {
            executionToken.GrantedToExecution = taskExecutionId;

            if (executionToken.Status != ExecutionTokenStatus.Unlimited)
                executionToken.Status = ExecutionTokenStatus.Unavailable;
        }

        private async Task PersistTokensAsync(int taskDefinitionId, ExecutionTokenList executionTokenList, SqlCommand command)
        {
            var tokenString = GenerateTokenString(executionTokenList);

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.UpdateExecutionTokensQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@ExecutionTokens", SqlDbType.VarChar, 8000).Value = tokenString;
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        private string GenerateTokenString(ExecutionTokenList executionTokenList)
        {
            var sb = new StringBuilder();
            int counter = 0;
            foreach (var token in executionTokenList.Tokens)
            {
                if (counter > 0)
                    sb.Append("|");

                sb.Append("I:");
                sb.Append(token.TokenId);
                sb.Append(",S:");
                sb.Append(((int)token.Status).ToString());
                sb.Append(",G:");
                sb.Append(token.GrantedToExecution);

                counter++;
            }

            return sb.ToString();
        }

        private void SetTokenAsAvailable(ExecutionTokenList executionTokenList, string executionTokenId)
        {
            var executionToken = executionTokenList.Tokens.FirstOrDefault(x => x.TokenId == executionTokenId);
            if (executionToken != null && executionToken.Status == ExecutionTokenStatus.Unavailable)
                executionToken.Status = ExecutionTokenStatus.Available;
        }
    }
}
