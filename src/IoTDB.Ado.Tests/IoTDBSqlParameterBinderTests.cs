using System.Data.Common;
using IoTSharp.Data.IoTDB;
using Xunit;

namespace IoTDB.Ado.Tests;

public class IoTDBSqlParameterBinderTests
{
    [Fact]
    public void Bind_QuotesValuesAndSkipsLiteralsAndComments()
    {
        var sql = IoTDBSqlParameterBinder.Bind(
            "SELECT * FROM root.db.device WHERE status = @status AND time >= @time AND note = '@status' -- @missing\n/* @also_missing */",
            new[]
            {
                new Apache.IoTDB.Data.IoTDBParameter("@status", "ready' OR 1=1 --"),
                new Apache.IoTDB.Data.IoTDBParameter("@time", new DateTime(2026, 11, 18, 12, 0, 0, DateTimeKind.Utc))
            });

        Assert.Contains("status = 'ready'' OR 1=1 --'", sql, StringComparison.Ordinal);
        Assert.Contains("time >= 1795003200000", sql, StringComparison.Ordinal);
        Assert.Contains("note = '@status' -- @missing\n/* @also_missing */", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Bind_RejectsMissingAndUnusedParameters()
    {
        Assert.Throws<InvalidOperationException>(() => IoTDBSqlParameterBinder.Bind(
            "SELECT * FROM root.db.device WHERE value = @missing",
            Array.Empty<DbParameter>()));
        Assert.Throws<InvalidOperationException>(() => IoTDBSqlParameterBinder.Bind(
            "SELECT * FROM root.db.device",
            new[] { new Apache.IoTDB.Data.IoTDBParameter("@unused", 1) }));
    }
}
