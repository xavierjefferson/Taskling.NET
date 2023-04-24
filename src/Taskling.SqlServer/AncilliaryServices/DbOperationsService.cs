using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Text.RegularExpressions;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.AncilliaryServices;

public class DbOperationsService
{
    protected async Task<SqlConnection> CreateNewConnectionAsync(TaskId taskId)
    {
        try
        {
            var connection = new SqlConnection(ConnectionStore.Instance.GetConnection(taskId).ConnectionString);
            await connection.OpenAsync().ConfigureAwait(false);

            return connection;
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    protected string LimitLength(string inputStr, int lengthLimit)
    {
        if (inputStr == null)
            return inputStr;

        if (inputStr.Length > lengthLimit)
            return inputStr.Substring(0, lengthLimit);

        return inputStr;
    }

    protected async Task BulkLoadInTransactionOperationAsync(DataTable dataTable, string tableNameAndSchema,
        SqlConnection connection, SqlTransaction transaction)
    {
        using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
        {
            foreach (DataColumn column in dataTable.Columns)
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

            bulkCopy.DestinationTableName = tableNameAndSchema;
            bulkCopy.BatchSize = 10000;
            try
            {
                await bulkCopy.WriteToServerAsync(dataTable).ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
                {
                    var pattern = @"\d+";
                    var match = Regex.Match(ex.Message, pattern);
                    var index = Convert.ToInt32(match.Value) - 1;

                    var fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    var sortedColumns = fi.GetValue(bulkCopy);
                    var items = (object[])sortedColumns.GetType()
                        .GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                    var itemdata = items[index].GetType()
                        .GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    var metadata = itemdata.GetValue(items[index]);

                    var column = metadata.GetType()
                        .GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(metadata);
                    var length = metadata.GetType()
                        .GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(metadata);
                    throw new ArgumentException(
                        string.Format("Column: {0} contains data with a length greater than: {1}", column, length));
                }

                throw;
            }
        }
    }

    protected void TryRollBack(SqlTransaction transaction, SqlException sqlEx)
    {
        try
        {
            transaction.Rollback();
        }
        catch (Exception)
        {
            throw new Exception(
                "Add failed. Error during transaction, then error during rollback. Rollback was NOT successfully executed",
                sqlEx);
        }

        if (TransientErrorDetector.IsTransient(sqlEx))
            throw new TransientException(
                "A transient exception has occurred. Add failed. Error during transaction. Rollback successfully executed",
                sqlEx);

        throw new Exception("Add failed. Error during transaction. Rollback successfully executed", sqlEx);
    }

    protected void TryRollback(SqlTransaction transaction, Exception ex)
    {
        try
        {
            transaction.Rollback();
        }
        catch (Exception)
        {
            throw new Exception(
                "Add failed. Error during transaction, then error during rollback. Rollback was NOT successfully executed",
                ex);
        }

        throw new Exception("Add failed. Error during transaction. Rollback successfully executed", ex);
    }
}