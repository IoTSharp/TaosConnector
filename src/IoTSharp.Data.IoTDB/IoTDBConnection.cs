using System.Data.Common;
using Apache.IoTDB.Data;

namespace IoTSharp.Data.IoTDB;

public sealed class IoTDBConnection : Apache.IoTDB.Data.IoTDBConnection
{
    public IoTDBConnection()
    {
    }

    public IoTDBConnection(string connectionString) : base(connectionString)
    {
    }

    public override IoTDBCommand CreateCommand() => new(this);

    public override IoTDBCommand CreateCommand(string commandText) => new(commandText, this);

    protected override DbCommand CreateDbCommand() => CreateCommand();

    protected override DbProviderFactory DbProviderFactory => IoTDBProviderFactory.Instance;
}
