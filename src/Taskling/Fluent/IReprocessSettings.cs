using System;
using Taskling.Tasks;

namespace Taskling.Fluent;

public interface IReprocessSettings
{
    string CurrentReferenceValue { get; set; }
    ReprocessOption ReprocessOption { get; set; }
    Guid ReferenceValueToReprocess { get; set; }
}