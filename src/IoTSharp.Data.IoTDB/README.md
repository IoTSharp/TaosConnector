# IoTSharp.Data.IoTDB

IoTSharp integration for Apache IoTDB 2.x. The package builds on the Apache `Apache.IoTDB.Data` ADO.NET driver and adds provider-factory registration, conservative Tree path/type mapping, and an attribute-based writer for simple telemetry entities.

## ADO.NET

```csharp
using System.Data.Common;
using Apache.IoTDB.Data;
using IoTSharp.Data.IoTDB;

IoTDBProvider.Register();
var factory = DbProviderFactories.GetFactory(IoTDBProvider.InvariantName);
using DbConnection connection = factory.CreateConnection()!;
connection.ConnectionString = "DataSource=127.0.0.1;Port=6667;Username=root;Password=root";
await connection.OpenAsync();

using DbCommand command = connection.CreateCommand();
command.CommandText = "SELECT temperature FROM root.factory.line01.sensor01 LIMIT 10";
using DbDataReader reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine(reader.GetValue(0));
```

Connection-string options are the Apache builder options: `DataSource`, `Port`, `Username`, `Password`, `FetchSize`, `Compression`, `PoolSize`, `ZoneId`, and `TimeOut`. IoTDB Tree databases are expressed as paths in SQL; they are not a per-connection database selection.

## Tree schema and ORM writer

```csharp
using Apache.IoTDB.Data;
using IoTSharp.Data.IoTDB;
using IoTSharp.ORM.IoTDB;

public sealed class Reading
{
    [IoTDBTimestamp] public DateTime Timestamp { get; init; }
    [IoTDBMeasurement("temperature")] public double Temperature { get; init; }
    [IoTDBMeasurement("status")] public string Status { get; init; } = "";
}

var device = "root.factory.line01.sensor01";
await using var orm = new IoTDBOrm("DataSource=127.0.0.1;Port=6667;Username=root;Password=root");
await orm.EnsureDatabaseAsync("root.factory");
await orm.EnsureTimeseriesAsync<Reading>(device);
await orm.InsertAsync(device, new Reading
{
    Timestamp = DateTime.UtcNow,
    Temperature = 21.5,
    Status = "ok"
});
```

`EnsureTimeseriesAsync` creates one series per measurement with `PLAIN` encoding and `LZ4` compression. `InsertAsync` binds the timestamp and values as parameters. Read and arbitrary SQL operations use the underlying `IoTDBConnection`/`IoTDBCommand` APIs. Updates and deletes should follow IoTDB's supported SQL semantics.

Only validated absolute Tree paths and simple measurement nodes are accepted by the schema SQL helpers. Supported CLR mappings are Boolean, signed/unsigned integers up to 64 bits, floating-point/decimal, date-time, string/char/Guid, byte arrays, and enums with integral backing types.

## Tests

Run offline tests with `dotnet test src/IoTDB.Ado.Tests/IoTDB.Ado.Tests.csproj`. Set `IOTDB_TEST_CONNECTION_STRING` to enable the optional live round-trip test against a disposable IoTDB 2.x instance; it uses and removes a uniquely named database. Without this variable the live test returns without contacting a server.
