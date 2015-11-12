using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Taskling.Client;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.SqlServer.CriticalSections
{
    public class CriticalSectionService : ICriticalSectionService
    {
        private readonly string _connectionString;
        private readonly int _connectTimeout;
        private readonly int _queryTimeout;

        public CriticalSectionService(ClientConnectionSettings clientConnectionSettings)
        {
            _connectionString = clientConnectionSettings.ConnectionString;
            _connectTimeout = (int)clientConnectionSettings.ConnectTimeout.TotalMilliseconds;
            _queryTimeout = (int)clientConnectionSettings.QueryTimeout.TotalMilliseconds;
        }

        public StartCriticalSectionResponse Start(StartCriticalSectionRequest startCriticalSectionRequest)
        {
            return RequestCriticalSectionToken(startCriticalSectionRequest);
        }

        public CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeCriticalSectionRequest)
        {
            return ReturnCriticalSectionToken(completeCriticalSectionRequest);
        }

        private StartCriticalSectionResponse RequestCriticalSectionToken(StartCriticalSectionRequest criticalSectionRequest)
        {
            var response = new StartCriticalSectionResponse();

            //using (var connection = new SqlConnection(_connectionString))
            //{
            //    connection.Open();

            //    SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

            //    var command = connection.CreateCommand();
            //    command.Transaction = myTransaction;
            //    command.CommandTimeout = _queryTimeout;
            //    command.CommandText = TokensQueryBuilder.RequestCriticalSectionTokenQuery;
            //    command.Parameters.Add("@TaskId", SqlDbType.Char, 32).Value = criticalSectionRequest.GetTaskId();
            //    command.Parameters.Add("@ExecutionId", SqlDbType.UniqueIdentifier).Value = criticalSectionRequest.ExecutionId;
            //    command.Parameters.Add("@SecondsOverride", SqlDbType.Int, 100).Value = criticalSectionRequest.SecondsOverride;

            //    try
            //    {
            //        using (var reader = command.ExecuteReader())
            //        {
            //            while (reader.Read())
            //            {
            //                response.GrantStatus = (GrantStatus)int.Parse(reader["Status"].ToString());
            //            }
            //        }
            //        myTransaction.Commit();
                    
            //        response.ResponseCode = ResponseCode.Ok;
            //    }
            //    catch (Exception ex)
            //    {
            //        try
            //        {
            //            myTransaction.Rollback();
            //        }
            //        catch (Exception)
            //        {
            //            throw new ExecutionException("Failed to commit critical section token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
            //        }

            //        throw new ExecutionException("Failed to commit critical section token grant request but successfully rolled back. Data is in a consistent state.", ex);
            //    }
            //}

            return response;
        }

        private CompleteCriticalSectionResponse ReturnCriticalSectionToken(CompleteCriticalSectionRequest completeCriticalSectionRequest)
        {
            var response = new CompleteCriticalSectionResponse();

            //using (var connection = new SqlConnection(_connectionString))
            //{
            //    connection.Open();
            //    SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

            //    var command = connection.CreateCommand();
            //    command.Transaction = myTransaction;
            //    command.CommandText = TokensQueryBuilder.ReturnCriticalSectionTokenQuery;
            //    command.CommandTimeout = _queryTimeout;
            //    command.Parameters.Add("@TaskId", SqlDbType.Char, 32).Value = completeCriticalSectionRequest.GetTaskId();
            //    command.Parameters.Add("@ExecutionId", SqlDbType.UniqueIdentifier).Value = completeCriticalSectionRequest.ExecutionId;

            //    try
            //    {
            //        command.ExecuteNonQuery();
            //        myTransaction.Commit();
            //    }
            //    catch (Exception ex)
            //    {
            //        try
            //        {
            //            myTransaction.Rollback();
            //        }
            //        catch (Exception)
            //        {
            //            throw new ExecutionException("Failed to commit critical section token return request but failed to roll back. Data could be in an inconsistent state.", ex);
            //        }

            //        throw new ExecutionException("Failed to commit critical section token return request but successfully rolled back. Data is in a consistent state.", ex);
            //    }
            //}

            return response;
        }
    }
}
