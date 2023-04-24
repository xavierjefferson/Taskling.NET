using System;

namespace Taskling.Exceptions;

[Serializable]
public class CouldNotStartException : Exception
{
    public CouldNotStartException(string message)
        : base(message)
    {
    }
}