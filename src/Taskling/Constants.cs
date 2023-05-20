using System.Linq;
using System.Reflection;
using Newtonsoft.Json;

namespace Taskling;

 
using System.Reflection;
using System.Linq;
using System.Runtime.CompilerServices;
using System.IO;
public class OrderedContractResolver : Newtonsoft.Json.Serialization.DefaultContractResolver
{
    protected override System.Collections.Generic.IList<Newtonsoft.Json.Serialization.JsonProperty> CreateProperties(System.Type type, Newtonsoft.Json.MemberSerialization memberSerialization)
    {
        var @base = base.CreateProperties(type, memberSerialization);
        var ordered = @base
            .OrderBy(p => p.Order ?? int.MaxValue)
            .ThenBy(p => p.PropertyName)
            .ToList();
        return ordered;
    }
}
public static class Constants
{
    private static OrderedContractResolver r = new OrderedContractResolver();
    public static string Serialize(object jsonObject)
    {
        var jsonSerializerSettings = new Newtonsoft.Json.JsonSerializerSettings
        {
            ContractResolver = r,
            Formatting = Formatting.Indented
        };

        using (var m = new StringWriter())
        {
            using (Newtonsoft.Json.JsonWriter writer = new Newtonsoft.Json.JsonTextWriter(m))
            {
                var serializer = Newtonsoft.Json.JsonSerializer.Create(jsonSerializerSettings);
                serializer.Serialize(writer, jsonObject);
                return m.ToString();
            }

        }
    }
    public const string CheckpointName = "Checkpoint";

    private static MethodBase GetRealMethodFromAsyncMethod(MethodBase asyncMethod)
    {
        var generatedType = asyncMethod.DeclaringType;
        var originalType = generatedType.DeclaringType;
        if (originalType == null)
        {
            return asyncMethod;
        }
        var matchingMethods =
            from methodInfo in originalType.GetMethods()
            let attr = methodInfo.GetCustomAttribute<AsyncStateMachineAttribute>()
            where attr != null && attr.StateMachineType == generatedType
            select methodInfo;

        // If this throws, the async method scanning failed.
        var foundMethod = matchingMethods.FirstOrDefault();
        return foundMethod ?? asyncMethod;
    }
    public static string GetEnteredMessage(MethodBase? method)
    {

        return $"Entered {GetRealMethodFromAsyncMethod(method)?.Name}";
    }
}