using System;
using Taskling.Enums;

namespace Taskling.Fluent;

public interface IReprocessSettings
{
    string CurrentReferenceValue { get; set; }
    ReprocessOptionEnum ReprocessOption { get; set; }
    Guid ReferenceValueToReprocess { get; set; }
}