using System.Data;
using System.Data.Common;
using IoTSharp.Data.IoTDB;
using IoTSharp.ORM.IoTDB;
using Xunit;

namespace IoTDB.Ado.Tests;

public class IoTDBProviderTests
{
    [Theory]
    [InlineData(typeof(bool), "BOOLEAN")]
    [InlineData(typeof(int), "INT32")]
    [InlineData(typeof(long), "INT64")]
    [InlineData(typeof(float), "FLOAT")]
    [InlineData(typeof(double), "DOUBLE")]
    [InlineData(typeof(decimal), "DOUBLE")]
    [InlineData(typeof(DateTime), "TIMESTAMP")]
    [InlineData(typeof(DateOnly), "DATE")]
    [InlineData(typeof(string), "TEXT")]
    [InlineData(typeof(byte[]), "BLOB")]
    public void TypeMapper_MapsTreeTypes(Type clrType, string expected) =>
        Assert.Equal(expected, IoTDBTreeSchema.ToDataTypeName(clrType));

    [Fact]
    public void TypeMapper_RejectsUnsupportedTypes() =>
        Assert.Throws<NotSupportedException>(() => IoTDBTreeSchema.ToDataTypeName(typeof(object)));

    [Fact]
    public void TypeMapper_UsesIoTDBDateAndTimestampTypes()
    {
        Assert.Equal("DATE", IoTDBTreeSchema.ToDataTypeName(DbType.Date));
        Assert.Equal("TIMESTAMP", IoTDBTreeSchema.ToDataTypeName(DbType.DateTime2));
    }

    [Fact]
    public void SchemaBuilder_ProducesTreeDdlAndParameterizedInsert()
    {
        Assert.Equal("CREATE DATABASE root.dbeco", IoTDBTreeSchema.CreateDatabaseSql("root.dbeco"));
        Assert.Equal(
            "CREATE TIMESERIES root.dbeco.device.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN, COMPRESSOR=LZ4",
            IoTDBTreeSchema.CreateTimeseriesSql("root.dbeco.device", "temperature", typeof(double)));
        Assert.Equal(
            "INSERT INTO root.dbeco.device (timestamp, temperature, status) VALUES (@timestamp, @p0, @p1)",
            IoTDBTreeSchema.CreateInsertSql("root.dbeco.device", new[] { "temperature", "status" }));
    }

    [Theory]
    [InlineData("root.db.device; DROP DATABASE root.db")]
    [InlineData("root.db.`device`")]
    [InlineData("device")]
    public void SchemaBuilder_RejectsUnsafeTreePaths(string path) =>
        Assert.Throws<ArgumentException>(() => IoTDBTreeSchema.ValidateDevicePath(path));

    [Fact]
    public void SchemaBuilder_RejectsUnsafeAndDuplicateMeasurements()
    {
        Assert.Throws<ArgumentException>(() => IoTDBTreeSchema.CreateTimeseriesSql("root.db.device", "temperature; DROP", typeof(double)));
        Assert.Throws<ArgumentException>(() => IoTDBTreeSchema.CreateInsertSql("root.db.device", new[] { "value", "VALUE" }));
    }

    [Fact]
    public void ProviderFactory_CreatesApacheAdoObjectsAndRegistersByInvariant()
    {
        IoTDBProvider.Register();
        var factory = DbProviderFactories.GetFactory(IoTDBProvider.InvariantName);

        Assert.Same(IoTDBProviderFactory.Instance, factory);
        Assert.IsType<IoTDBConnection>(factory.CreateConnection());
        Assert.IsType<IoTDBCommand>(factory.CreateCommand());
        Assert.IsType<Apache.IoTDB.Data.IoTDBParameter>(factory.CreateParameter());
        Assert.IsType<Apache.IoTDB.Data.IoTDBConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task RemoteSmoke_CreatesWritesReadsAndDropsTreeDatabase()
    {
        var connectionString = Environment.GetEnvironmentVariable("IOTDB_TEST_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString)) return;

        var database = "root.dbeco_iotsharp_" + Guid.NewGuid().ToString("N");
        var device = database + ".device";
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).UtcDateTime;
        await using var orm = new IoTDBOrm(connectionString);
        var created = false;
        try
        {
            await orm.EnsureDatabaseAsync(database);
            created = true;
            await orm.EnsureTimeseriesAsync<RemoteReading>(device);
            await orm.EnsureDatabaseAsync(database);
            await orm.EnsureTimeseriesAsync<RemoteReading>(device);
            await orm.InsertAsync(device, new RemoteReading { Timestamp = timestamp, Temperature = 18.75, Status = "ready" });

            using var command = orm.Connection.CreateCommand($"SELECT temperature, status FROM {device} WHERE time = @timestamp");
            command.Parameters.Add(new Apache.IoTDB.Data.IoTDBParameter("@timestamp", timestamp));
            using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(timestamp, reader.GetDateTime(0).ToUniversalTime());
            Assert.Equal(18.75, Convert.ToDouble(reader.GetValue(1)), 6);
            Assert.Equal("ready", Convert.ToString(reader.GetValue(2)));
        }
        finally
        {
            if (created)
            {
                using var command = orm.Connection.CreateCommand($"DROP DATABASE {database}");
                await command.ExecuteNonQueryAsync();
            }
        }
    }

    private sealed class RemoteReading
    {
        [IoTDBTimestamp]
        public DateTime Timestamp { get; init; }

        [IoTDBMeasurement("temperature")]
        public double Temperature { get; init; }

        [IoTDBMeasurement("status")]
        public string Status { get; init; } = string.Empty;
    }
}
