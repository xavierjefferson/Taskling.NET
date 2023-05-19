using System;
using Newtonsoft.Json;
using Taskling.Exceptions;

namespace Taskling.Serialization;

public class JsonGenericSerializer
{
    public static string Serialize<T>(T data)
    {
        // _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (data == null)
            throw new ExecutionException("The object being serialized is null");

        return JsonConvert.SerializeObject(data);
    }

    public static T Deserialize<T>(string input, bool allowNullValues = false)
    {
        // _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (input == null)
        {
            if (allowNullValues)
                return default;

            throw new ExecutionException("The object being deserialized is null");
        }

        try
        {
            return JsonConvert.DeserializeObject<T>(input);
        }
        catch (Exception ex)
        {
            throw new ExecutionException(
                "The object type being deserialized is not compatible with the specified type. This could happen if you change the type of an existing process for a different type, or if you make non retro compatible changes to the exsiting type, for example by removing properties.",
                ex);
        }
    }
}