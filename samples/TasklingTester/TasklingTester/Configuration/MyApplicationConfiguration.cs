namespace TasklingTester.Configuration;

public class MyApplicationConfiguration : IMyApplicationConfiguration
{
    public DateTime FirstRunDate => DateTime.Now.AddHours(-3);
}