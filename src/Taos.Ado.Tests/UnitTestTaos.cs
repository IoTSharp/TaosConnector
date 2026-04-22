using DotNet.Testcontainers.Builders;
using IoTSharp.Data.Taos;
using Newtonsoft.Json.Linq;
using System;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;

namespace Taos.Ado.Tests
{
    /// <summary>
    /// Fixture that manages a TDengine Docker container for integration tests.
    /// Requires Docker to be running.
    /// </summary>
    public class TDengineFixture : IAsyncLifetime
    {
        private DotNet.Testcontainers.Containers.IContainer _container;
        public string Host { get; private set; } = "127.0.0.1";
        public int NativePort { get; private set; } = 6030;
        public int AdapterPort { get; private set; } = 6041;
        public string Database { get; } = "db_" + DateTime.Now.ToString("yyyyMMddHHmmss");

        public async Task InitializeAsync()
        {
            _container = new ContainerBuilder()
                .WithImage("tdengine/tdengine:latest")
                .WithPortBinding(6030, true)
                .WithPortBinding(6041, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r =>
                    r.ForPort(6041).ForPath("/rest/login/root/taosdata")))
                .Build();

            await _container.StartAsync();
            Host = _container.Hostname;
            NativePort = _container.GetMappedPublicPort(6030);
            AdapterPort = _container.GetMappedPublicPort(6041);

            // Wait for taosadapter to be ready
            await Task.Delay(TimeSpan.FromSeconds(5));

            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
            DbProviderFactories.RegisterFactory("TDengine", TaosFactory.Instance);

            // Register native library resolver
            try
            {
                NativeLibrary.SetDllImportResolver(typeof(TDengineFixture).Assembly, DllImportResolver);
            }
            catch (InvalidOperationException)
            {
                // resolver already registered
            }

            // Create test database
            var builder = GetNativeBuilder();
            using var connection = new TaosConnection(builder.ConnectionString);
            connection.Open();
            connection.CreateCommand($"create database {Database};").ExecuteNonQuery();
        }

        public async Task DisposeAsync()
        {
            if (_container != null)
                await _container.DisposeAsync();
        }

        private static IntPtr DllImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (libraryName == "taos")
            {
                if (Environment.OSVersion.Platform == PlatformID.Win32NT && Environment.Is64BitProcess)
                    return NativeLibrary.Load("taos_win_x64.dll");
                else if (Environment.OSVersion.Platform == PlatformID.Win32NT && !Environment.Is64BitProcess)
                    return NativeLibrary.Load("taos_win_x86.dll");
                else if (Environment.OSVersion.Platform == PlatformID.Unix && Environment.Is64BitProcess)
                    return NativeLibrary.Load("libtaos_linux_x64.so");
            }
            return IntPtr.Zero;
        }

        public TaosConnectionStringBuilder GetNativeBuilder() =>
            new TaosConnectionStringBuilder
            {
                DataSource = Host,
                DataBase = Database,
                Username = "root",
                Password = "taosdata",
                Port = NativePort
            }.UseNative();

        public TaosConnectionStringBuilder GetWebSocketBuilder() =>
            new TaosConnectionStringBuilder
            {
                DataSource = Host,
                DataBase = Database,
                Username = "root",
                Password = "taosdata",
                Port = AdapterPort
            }.UseWebSocket();

        public TaosConnectionStringBuilder GetRESTfulBuilder() =>
            new TaosConnectionStringBuilder
            {
                DataSource = Host,
                DataBase = Database,
                Username = "root",
                Password = "taosdata",
                Port = AdapterPort
            }.UseRESTful();
    }

    [Collection("TDengine")]
    public class WebSocketTests : IClassFixture<TDengineFixture>
    {
        private readonly TDengineFixture _fixture;

        public WebSocketTests(TDengineFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void TestWebSocketConnect()
        {
            using var connection = new TaosConnection(_fixture.GetWebSocketBuilder().ConnectionString);
            connection.Open();
            Assert.Equal(System.Data.ConnectionState.Open, connection.State);
        }

        [Fact]
        public void TestWebSocketCreateAndQueryTable()
        {
            using var connection = new TaosConnection(_fixture.GetWebSocketBuilder().ConnectionString);
            connection.Open();
            connection.ChangeDatabase(_fixture.Database);

            var tableName = $"ws_test_{DateTime.Now:HHmmss}";
            connection.CreateCommand($"create table {tableName}(ts timestamp, val int);").ExecuteNonQuery();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            connection.CreateCommand($"insert into {tableName} values({ts}, 42);").ExecuteNonQuery();

            using var reader = connection.CreateCommand($"select * from {tableName};").ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("val")));
        }

        [Fact]
        public void TestWebSocketSchemalessInsert()
        {
            using var connection = new TaosConnection(_fixture.GetWebSocketBuilder().ConnectionString);
            connection.Open();
            connection.ChangeDatabase(_fixture.Database);

            string[] lines = {
                $"ws_meters,location=Beijing,groupid=1 current=11.8,voltage=221 {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"
            };
            var result = connection.CreateCommand().ExecuteLineBulkInsert(lines);
            Assert.Equal(lines.Length, result);
        }

        [Fact]
        public void TestWebSocketStmtInsert()
        {
            using var connection = new TaosConnection(_fixture.GetWebSocketBuilder().ConnectionString);
            connection.Open();
            connection.ChangeDatabase(_fixture.Database);

            var tableName = $"ws_stmt_{DateTime.Now:HHmmss}";
            connection.CreateCommand($"create table {tableName}(ts timestamp, val int, name binary(32));").ExecuteNonQuery();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"insert into {tableName} values(@ts, @val, @name)";
            cmd.Parameters.Add(new TaosParameter("@ts", DateTime.UtcNow));
            cmd.Parameters.Add(new TaosParameter("@val", 100));
            cmd.Parameters.Add(new TaosParameter("@name", "hello"));
            cmd.ExecuteNonQuery();

            using var reader = connection.CreateCommand($"select val, name from {tableName};").ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(100, reader.GetInt32(0));
            Assert.Equal("hello", reader.GetString(1));
        }

        [Fact]
        public void TestWebSocketShowDatabases()
        {
            var builder = _fixture.GetWebSocketBuilder();
            builder.DataBase = null;
            using var connection = new TaosConnection(builder.ConnectionString);
            connection.Open();
            using var cmd = connection.CreateCommand("show databases");
            using var reader = cmd.ExecuteReader();
            var found = false;
            while (reader.Read())
            {
                if (reader.GetString(0) == _fixture.Database)
                {
                    found = true;
                    break;
                }
            }
            Assert.True(found, $"Database '{_fixture.Database}' not found in 'show databases'");
        }
    }

    [Collection("TDengine")]
    public class RESTfulTests : IClassFixture<TDengineFixture>
    {
        private readonly TDengineFixture _fixture;

        public RESTfulTests(TDengineFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void TestRESTfulConnect()
        {
            using var connection = new TaosConnection(_fixture.GetRESTfulBuilder().ConnectionString);
            connection.Open();
            Assert.Equal(System.Data.ConnectionState.Open, connection.State);
        }

        [Fact]
        public void TestRESTfulCreateAndQueryTable()
        {
            using var connection = new TaosConnection(_fixture.GetRESTfulBuilder().ConnectionString);
            connection.Open();
            connection.ChangeDatabase(_fixture.Database);

            var tableName = $"rest_test_{DateTime.Now:HHmmss}";
            connection.CreateCommand($"create table {tableName}(ts timestamp, val int);").ExecuteNonQuery();

            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            connection.CreateCommand($"insert into {tableName} values({ts}, 99);").ExecuteNonQuery();

            using var reader = connection.CreateCommand($"select * from {tableName};").ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("val")));
        }
    }
}
