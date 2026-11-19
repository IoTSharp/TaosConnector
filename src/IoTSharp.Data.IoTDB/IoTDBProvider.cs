using System.Data.Common;

namespace IoTSharp.Data.IoTDB;

public static class IoTDBProvider
{
    public const string InvariantName = "IoTSharp.Data.IoTDB";

    public static void Register() => DbProviderFactories.RegisterFactory(InvariantName, IoTDBProviderFactory.Instance);
}
