using System;

namespace Taskling.Exceptions;

public class ExecutionArgumentsException : Exception
{
    public ExecutionArgumentsException(string message)
        : base(message)
    {
    }

    public ExecutionArgumentsException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}