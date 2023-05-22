namespace Taskling.EntityFrameworkCore.Configuration;

[Serializable]
public class TokenFormatException : Exception
{
    public TokenFormatException(string message)
        : base(message)
    {
    }
}