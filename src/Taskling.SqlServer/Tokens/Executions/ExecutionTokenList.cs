using System.Text;
using Newtonsoft.Json;
using Taskling.SqlServer.Configuration;

namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionTokenList:List<ExecutionToken>
{
    //public ExecutionTokenList()
    //{
    
    //}

    //public ExecutionTokenList(IEnumerable<ExecutionToken> executionTokens) : base(executionTokens)
    //{

    //}

    public string Serialize()
    {
        return JsonConvert.SerializeObject(this);
    }

    public static ExecutionTokenList Deserialize(string tokensString)
    {
        return JsonConvert.DeserializeObject<ExecutionTokenList>(tokensString);
    }
}