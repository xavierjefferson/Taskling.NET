﻿using System;

namespace Taskling.Exceptions;

public class TransientException : Exception
{
    public TransientException(string message)
        : base(message)
    {
    }

    public TransientException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}