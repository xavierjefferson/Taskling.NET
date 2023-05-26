using Taskling.Enums;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class GetLastEventResponse
{
    public GetLastEventResponse(EventTypeEnum eventType, string message)
    {
        EventType = eventType;
        Message = message;
    }

    public EventTypeEnum EventType { get; }
    public string Message { get; }
}