using System.Data.Common;
using Apache.IoTDB.Data;

namespace IoTSharp.Data.IoTDB;

public sealed class IoTDBProviderFactory : DbProviderFactory
{
    public static readonly IoTDBProviderFactory Instance = new();

    private IoTDBProviderFactory()
    {
    }

    public override DbCommand CreateCommand() => new IoTDBCommand();

    public override DbConnection CreateConnection() => new IoTDBConnection();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new IoTDBConnectionStringBuilder();

    public override DbParameter CreateParameter() => new IoTDBParameter();
}
