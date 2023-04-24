using System;

namespace Taskling.Exceptions;

[Serializable]
public class TaskConfigurationException : Exception
{
    public TaskConfigurationException(string message)
        : base(message)
    {
    }

    public TaskConfigurationException(string message, Exception ex)
        : base(message, ex)
    {
    }
}