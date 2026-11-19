using System.Data;
using System.Reflection;
using IoTSharp.Data.IoTDB;

namespace IoTSharp.ORM.IoTDB;

public sealed class IoTDBOrm : IDisposable, IAsyncDisposable
{
    private readonly IoTDBConnection _connection;
    private readonly bool _ownsConnection;

    public IoTDBOrm(string connectionString) : this(new IoTDBConnection(connectionString), true)
    {
    }

    public IoTDBOrm(IoTDBConnection connection) : this(connection, false)
    {
    }

    private IoTDBOrm(IoTDBConnection connection, bool ownsConnection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _ownsConnection = ownsConnection;
    }

    public IoTDBConnection Connection => _connection;

    public async Task EnsureDatabaseAsync(string databasePath, CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
        await EnsureSchemaAsync(IoTDBTreeSchema.CreateDatabaseSql(databasePath), $"SHOW DATABASES {databasePath}", cancellationToken).ConfigureAwait(false);
    }

    public async Task EnsureTimeseriesAsync<T>(string devicePath, CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
        var mapping = GetMapping<T>();
        _ = IoTDBTreeSchema.CreateInsertSql(devicePath, mapping.Measurements.Select(item => item.Name).ToArray());
        foreach (var measurement in mapping.Measurements)
        {
            var sql = IoTDBTreeSchema.CreateTimeseriesSql(devicePath, measurement.Name, measurement.Property.PropertyType);
            await EnsureSchemaAsync(sql, $"SHOW TIMESERIES {devicePath}.{measurement.Name}", cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task EnsureSchemaAsync(string ddl, string lookup, CancellationToken cancellationToken)
    {
        async Task<bool> ExistsAsync()
        {
            using var check = _connection.CreateCommand(lookup);
            using var rows = await check.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await rows.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        if (await ExistsAsync().ConfigureAwait(false)) return;
        using var create = _connection.CreateCommand(ddl);
        try { await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false); }
        catch (DataException) { if (!await ExistsAsync().ConfigureAwait(false)) throw; }
    }

    public async Task<int> InsertAsync<T>(string devicePath, T entity, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entity);
        await EnsureOpenAsync(cancellationToken).ConfigureAwait(false);

        var mapping = GetMapping<T>();
        var timestamp = mapping.Timestamp.GetValue(entity) ?? throw new InvalidOperationException("The mapped IoTDB timestamp property cannot be null.");
        var sql = IoTDBTreeSchema.CreateInsertSql(devicePath, mapping.Measurements.Select(item => item.Name).ToArray());
        using var command = _connection.CreateCommand(sql);
        command.Parameters.Add(new Apache.IoTDB.Data.IoTDBParameter("@timestamp", timestamp));
        for (var index = 0; index < mapping.Measurements.Count; index++)
        {
            var measurement = mapping.Measurements[index];
            var value = measurement.Property.GetValue(entity) ?? DBNull.Value;
            command.Parameters.Add(new Apache.IoTDB.Data.IoTDBParameter($"@p{index}", value));
        }

        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureOpenAsync(CancellationToken cancellationToken)
    {
        if (_connection.State != ConnectionState.Open)
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    private static EntityMapping GetMapping<T>()
    {
        var properties = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var timestamps = properties.Where(property => property.IsDefined(typeof(IoTDBTimestampAttribute), true)).ToArray();
        if (timestamps.Length != 1)
            throw new InvalidOperationException($"'{typeof(T).Name}' must have exactly one property marked with IoTDBTimestampAttribute.");
        if (timestamps[0].PropertyType != typeof(DateTime) && timestamps[0].PropertyType != typeof(DateTimeOffset))
            throw new InvalidOperationException("The IoTDB timestamp property must be DateTime or DateTimeOffset.");

        var measurements = properties
            .Select(property => (Property: property, Attribute: property.GetCustomAttribute<IoTDBMeasurementAttribute>()))
            .Where(item => item.Attribute != null)
            .Select(item => new MeasurementMapping(
                item.Property,
                string.IsNullOrWhiteSpace(item.Attribute.Name) ? item.Property.Name : item.Attribute.Name))
            .ToArray();
        if (measurements.Length == 0)
            throw new InvalidOperationException($"'{typeof(T).Name}' must have at least one property marked with IoTDBMeasurementAttribute.");

        return new EntityMapping(timestamps[0], measurements);
    }

    public void Dispose()
    {
        if (_ownsConnection) _connection.Dispose();
    }

    public ValueTask DisposeAsync() => _ownsConnection ? _connection.DisposeAsync() : ValueTask.CompletedTask;

    private sealed record EntityMapping(PropertyInfo Timestamp, IReadOnlyList<MeasurementMapping> Measurements);
    private sealed record MeasurementMapping(PropertyInfo Property, string Name);
}
