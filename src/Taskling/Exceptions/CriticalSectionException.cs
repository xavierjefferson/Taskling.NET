using System;

namespace Taskling.Exceptions;

[Serializable]
public class CriticalSectionException : Exception
{
    public CriticalSectionException(string message)
        : base(message)
    {
    }
}