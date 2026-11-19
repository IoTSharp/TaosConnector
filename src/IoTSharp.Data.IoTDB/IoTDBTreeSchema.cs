using System.Data;
using System.Text.RegularExpressions;

namespace IoTSharp.Data.IoTDB;

public static partial class IoTDBTreeSchema
{
    [GeneratedRegex(@"^root(?:\.[\p{L}_$][\p{L}\p{N}_$-]*)+$", RegexOptions.CultureInvariant)]
    private static partial Regex TreePathPattern();

    [GeneratedRegex(@"^[\p{L}_$][\p{L}\p{N}_$-]*$", RegexOptions.CultureInvariant)]
    private static partial Regex NodePattern();

    public static string ValidateDevicePath(string devicePath)
    {
        if (string.IsNullOrWhiteSpace(devicePath) || !TreePathPattern().IsMatch(devicePath))
            throw new ArgumentException("Use an absolute IoTDB device path such as root.factory.line01.sensor01.", nameof(devicePath));

        return devicePath;
    }

    public static string ValidateDatabasePath(string databasePath)
    {
        var path = ValidateDevicePath(databasePath);
        if (path.Count(character => character == '.') != 1)
            throw new ArgumentException("An IoTDB database path must be a direct child of root, such as root.factory.", nameof(databasePath));

        return path;
    }

    public static string ToDataTypeName(Type clrType)
    {
        ArgumentNullException.ThrowIfNull(clrType);
        var type = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (type.IsEnum) type = Enum.GetUnderlyingType(type);

        if (type == typeof(bool)) return "BOOLEAN";
        if (type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort) || type == typeof(int)) return "INT32";
        if (type == typeof(uint) || type == typeof(long) || type == typeof(ulong)) return "INT64";
        if (type == typeof(float)) return "FLOAT";
        if (type == typeof(double) || type == typeof(decimal)) return "DOUBLE";
        if (type == typeof(DateTime) || type == typeof(DateTimeOffset)) return "TIMESTAMP";
        if (type == typeof(DateOnly)) return "DATE";
        if (type == typeof(byte[])) return "BLOB";
        if (type == typeof(string) || type == typeof(char) || type == typeof(Guid)) return "TEXT";

        throw new NotSupportedException($"CLR type '{clrType.FullName}' has no IoTDB Tree datatype mapping.");
    }

    public static string ToDataTypeName(DbType dbType) => dbType switch
    {
        DbType.Boolean => "BOOLEAN",
        DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32 => "INT32",
        DbType.UInt32 or DbType.Int64 or DbType.UInt64 => "INT64",
        DbType.Single => "FLOAT",
        DbType.Double or DbType.Decimal or DbType.Currency or DbType.VarNumeric => "DOUBLE",
        DbType.Date => "DATE",
        DbType.DateTime or DbType.DateTime2 or DbType.DateTimeOffset => "TIMESTAMP",
        DbType.Binary => "BLOB",
        DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength or DbType.Guid => "TEXT",
        _ => throw new NotSupportedException($"DbType '{dbType}' has no IoTDB Tree datatype mapping.")
    };

    public static string CreateDatabaseSql(string databasePath) => $"CREATE DATABASE {ValidateDatabasePath(databasePath)}";

    public static string CreateTimeseriesSql(string devicePath, string measurement, Type clrType)
    {
        var path = ValidateDevicePath(devicePath);
        var node = ValidateNode(measurement, nameof(measurement));
        return $"CREATE TIMESERIES {path}.{node} WITH DATATYPE={ToDataTypeName(clrType)}, ENCODING=PLAIN, COMPRESSOR=LZ4";
    }

    public static string CreateInsertSql(string devicePath, IReadOnlyList<string> measurements)
    {
        ArgumentNullException.ThrowIfNull(measurements);
        if (measurements.Count == 0) throw new ArgumentException("At least one measurement is required.", nameof(measurements));

        var path = ValidateDevicePath(devicePath);
        var nodes = measurements.Select((measurement, index) => ValidateNode(measurement, $"measurements[{index}]")).ToArray();
        if (nodes.Distinct(StringComparer.OrdinalIgnoreCase).Count() != nodes.Length)
            throw new ArgumentException("Measurement names must be unique.", nameof(measurements));

        var columns = string.Join(", ", new[] { "timestamp" }.Concat(nodes));
        var parameters = string.Join(", ", new[] { "@timestamp" }.Concat(Enumerable.Range(0, nodes.Length).Select(index => $"@p{index}")));
        return $"INSERT INTO {path} ({columns}) VALUES ({parameters})";
    }

    private static string ValidateNode(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value) || !NodePattern().IsMatch(value))
            throw new ArgumentException("IoTDB path nodes may contain letters, digits, underscore, dollar, and hyphen, and must start with a letter, underscore, or dollar.", parameterName);

        return value;
    }
}
