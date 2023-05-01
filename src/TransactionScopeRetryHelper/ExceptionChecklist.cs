using Polly;

namespace TransactionScopeRetryHelper;

public interface IPollyCheck
{
    public PolicyBuilder<T> Add<T>(PolicyBuilder<T> input);
    public PolicyBuilder Add(PolicyBuilder input);
}


public class ExceptionChecklist : List<Func<Exception, bool>>
{
}

public class PollyExtension : List<IPollyCheck>
{

}