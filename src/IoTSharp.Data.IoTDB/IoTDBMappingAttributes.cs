namespace IoTSharp.ORM.IoTDB;

[AttributeUsage(AttributeTargets.Property)]
public sealed class IoTDBTimestampAttribute : Attribute
{
}

[AttributeUsage(AttributeTargets.Property)]
public sealed class IoTDBMeasurementAttribute : Attribute
{
    public IoTDBMeasurementAttribute(string name = null) => Name = name;

    public string Name { get; }
}
