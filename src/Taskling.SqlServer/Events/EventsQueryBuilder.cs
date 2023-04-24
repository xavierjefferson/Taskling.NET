namespace Taskling.SqlServer.Events;

internal class EventsQueryBuilder
{
    public const string InsertTaskExecutionEventQuery = @"
INSERT INTO [Taskling].[TaskExecutionEvent]
           ([TaskExecutionId]
           ,[EventType]
           ,[Message]
           ,[EventDateTime])
     VALUES
           (@TaskExecutionId
           ,@EventType
           ,@Message
           ,@EventDateTime)";
}