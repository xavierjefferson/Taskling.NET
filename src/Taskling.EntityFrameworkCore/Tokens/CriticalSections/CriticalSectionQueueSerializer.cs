using Newtonsoft.Json;

namespace Taskling.EntityFrameworkCore.Tokens.CriticalSections;

public class CriticalSectionQueueSerializer
{
    public static List<CriticalSectionQueueItem> Deserialize(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return new List<CriticalSectionQueueItem>();
        return JsonConvert.DeserializeObject<List<CriticalSectionQueueItem>>(json);
    }

    public static string Serialize(IEnumerable<CriticalSectionQueueItem> items)
    {
        return JsonConvert.SerializeObject(items);
    }
}