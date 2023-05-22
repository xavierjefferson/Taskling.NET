using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Taskling;

public class OrderedContractResolver : DefaultContractResolver
{
    protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
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
    public const string CheckpointName = "Checkpoint";
    private static readonly OrderedContractResolver r = new();

    public static string Serialize(object jsonObject)
    {
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = r,
            Formatting = Formatting.Indented
        };

        using (var m = new StringWriter())
        {
            using (JsonWriter writer = new JsonTextWriter(m))
            {
                var serializer = JsonSerializer.Create(jsonSerializerSettings);
                serializer.Serialize(writer, jsonObject);
                return m.ToString();
            }
        }
    }

    private static MethodBase GetRealMethodFromAsyncMethod(MethodBase asyncMethod)
    {
        var generatedType = asyncMethod.DeclaringType;
        var originalType = generatedType.DeclaringType;
        if (originalType == null) return asyncMethod;
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
        return $"Entered {method?.Name}";
    }
}