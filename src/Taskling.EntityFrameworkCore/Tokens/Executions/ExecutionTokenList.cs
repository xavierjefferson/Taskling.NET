using Newtonsoft.Json;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class ExecutionTokenList : List<ExecutionToken>
{
    public string Serialize()
    {
        return JsonConvert.SerializeObject(this);
    }

    public static ExecutionTokenList Deserialize(string tokensString)
    {
        return JsonConvert.DeserializeObject<ExecutionTokenList>(tokensString);
    }
}