using Taskling.Tasks;

namespace Taskling.Fluent;

public interface IReprocessSettings
{
    string CurrentReferenceValue { get; set; }
    ReprocessOption ReprocessOption { get; set; }
    string ReferenceValueToReprocess { get; set; }
}