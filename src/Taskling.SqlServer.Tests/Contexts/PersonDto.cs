using System;

namespace Taskling.SqlServer.Tests.Contexts;

public class PersonDto
{
    public int Id { get; set; }
    public string Name { get; set; }
    public DateTime DateOfBirth { get; set; }
}