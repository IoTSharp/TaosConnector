using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Apache.IoTDB.Data;

namespace IoTSharp.Data.IoTDB;

public sealed class IoTDBCommand : Apache.IoTDB.Data.IoTDBCommand
{
    public IoTDBCommand()
    {
    }

    public IoTDBCommand(string commandText) : base(commandText)
    {
    }

    public IoTDBCommand(IoTDBConnection connection) : base()
    {
        Connection = connection;
    }

    public IoTDBCommand(string commandText, IoTDBConnection connection) : base(commandText, connection)
    {
    }

    public override IoTDBDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

    public override IoTDBDataReader ExecuteReader(CommandBehavior behavior)
    {
        using var command = CreateNativeCommand();
        return command.ExecuteReader(behavior);
    }

    public override Task<IoTDBDataReader> ExecuteReaderAsync() => ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

    public override Task<IoTDBDataReader> ExecuteReaderAsync(CancellationToken cancellationToken) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    public override Task<IoTDBDataReader> ExecuteReaderAsync(CommandBehavior behavior) =>
        ExecuteReaderAsync(behavior, CancellationToken.None);

    public override Task<IoTDBDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteReader(behavior));
    }

    public override int ExecuteNonQuery()
    {
        using var command = CreateNativeCommand();
        var result = command.ExecuteNonQuery();
        if (result < 0) throw new DataException("IoTDB rejected the non-query statement.");
        return result;
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteNonQuery());
    }

    public override object ExecuteScalar()
    {
        using var command = CreateNativeCommand();
        return command.ExecuteScalar();
    }

    public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteScalar());
    }

    private Apache.IoTDB.Data.IoTDBCommand CreateNativeCommand()
    {
        if (Connection == null) throw new InvalidOperationException("An IoTDB connection is required before executing a command.");
        if (string.IsNullOrWhiteSpace(CommandText)) throw new InvalidOperationException("CommandText must be set before execution.");

        var parameters = Parameters.Cast<DbParameter>().ToArray();
        var command = new Apache.IoTDB.Data.IoTDBCommand(
            IoTDBSqlParameterBinder.Bind(CommandText, parameters),
            Connection)
        {
            CommandTimeout = CommandTimeout,
            Transaction = Transaction
        };
        return command;
    }
}

public static class IoTDBSqlParameterBinder
{
    public static string Bind(string sql, IEnumerable<DbParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(sql);
        var parameterMap = new Dictionary<string, DbParameter>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in parameters ?? Enumerable.Empty<DbParameter>())
        {
            if (parameter == null) continue;
            if (parameter.Direction != ParameterDirection.Input)
                throw new NotSupportedException("IoTDB SQL parameters support input values only.");

            var name = NormalizeName(parameter.ParameterName);
            if (name.Length == 0) throw new InvalidOperationException("IoTDB SQL parameters must have a name.");
            if (!parameterMap.TryAdd(name, parameter))
                throw new InvalidOperationException($"Duplicate IoTDB SQL parameter '{name}'.");
        }

        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new StringBuilder(sql.Length + parameterMap.Count * 12);
        var quote = '\0';
        var lineComment = false;
        var blockComment = false;
        for (var index = 0; index < sql.Length; index++)
        {
            var current = sql[index];
            var next = index + 1 < sql.Length ? sql[index + 1] : '\0';

            if (lineComment)
            {
                result.Append(current);
                if (current == '\n') lineComment = false;
                continue;
            }
            if (blockComment)
            {
                result.Append(current);
                if (current == '*' && next == '/')
                {
                    result.Append(next);
                    index++;
                    blockComment = false;
                }
                continue;
            }
            if (quote != '\0')
            {
                result.Append(current);
                if (current == quote)
                {
                    if (next == quote)
                    {
                        result.Append(next);
                        index++;
                    }
                    else quote = '\0';
                }
                else if (current == '\\' && next != '\0')
                {
                    result.Append(next);
                    index++;
                }
                continue;
            }

            if (current == '-' && next == '-')
            {
                result.Append(current).Append(next);
                index++;
                lineComment = true;
                continue;
            }
            if (current == '/' && next == '*')
            {
                result.Append(current).Append(next);
                index++;
                blockComment = true;
                continue;
            }
            if (current is '\'' or '"' or '`')
            {
                quote = current;
                result.Append(current);
                continue;
            }
            if ((current is '@' or ':' or '$') && IsIdentifierStart(next))
            {
                var start = index + 1;
                var end = start + 1;
                while (end < sql.Length && IsIdentifierPart(sql[end])) end++;
                var name = sql[start..end];
                if (!parameterMap.TryGetValue(name, out var parameter))
                    throw new InvalidOperationException($"No value was supplied for IoTDB SQL parameter '{name}'.");

                result.Append(ToLiteral(parameter.Value));
                used.Add(name);
                index = end - 1;
                continue;
            }
            result.Append(current);
        }

        var unused = parameterMap.Keys.Where(name => !used.Contains(name)).ToArray();
        if (unused.Length > 0)
            throw new InvalidOperationException($"IoTDB SQL parameters were not bound: {string.Join(", ", unused)}.");
        return result.ToString();
    }

    public static string ToLiteral(object value)
    {
        if (value == null || value == DBNull.Value) return "NULL";
        if (value is string text) return QuoteString(text);
        if (value is char character) return QuoteString(character.ToString());
        if (value is bool boolean) return boolean ? "true" : "false";
        if (value is DateTime dateTime)
        {
            var utc = dateTime.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc) : dateTime.ToUniversalTime();
            return new DateTimeOffset(utc).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
        }
        if (value is DateTimeOffset dateTimeOffset) return dateTimeOffset.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
        if (value is DateOnly dateOnly) return QuoteString(dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        if (value is byte[] bytes) return "X'" + Convert.ToHexString(bytes) + "'";
        if (value is Guid guid) return QuoteString(guid.ToString("D"));
        if (value is Enum enumValue) return Convert.ToInt64(enumValue, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
        if (value is float single)
        {
            if (!float.IsFinite(single)) throw new NotSupportedException("IoTDB SQL does not accept non-finite numeric literals.");
            return single.ToString("R", CultureInfo.InvariantCulture);
        }
        if (value is double doubleValue)
        {
            if (!double.IsFinite(doubleValue)) throw new NotSupportedException("IoTDB SQL does not accept non-finite numeric literals.");
            return doubleValue.ToString("R", CultureInfo.InvariantCulture);
        }
        if (value is decimal decimalValue) return decimalValue.ToString(CultureInfo.InvariantCulture);
        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            if (value is ulong unsigned && unsigned > long.MaxValue)
                throw new NotSupportedException("IoTDB INT64 measurements cannot store unsigned values above Int64.MaxValue.");
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }
        if (value is System.Collections.IEnumerable)
            throw new NotSupportedException("Collection values cannot be bound to a scalar IoTDB SQL parameter.");

        throw new NotSupportedException($"IoTDB SQL parameter type '{value.GetType().FullName}' is not supported.");
    }

    private static string NormalizeName(string name)
    {
        var value = (name ?? string.Empty).Trim();
        return value.Length > 0 && (value[0] is '@' or ':' or '$') ? value[1..] : value;
    }

    private static string QuoteString(string value) => "'" + value.Replace("'", "''") + "'";

    private static bool IsIdentifierStart(char value) => value == '_' || char.IsLetter(value);

    private static bool IsIdentifierPart(char value) => value == '_' || char.IsLetterOrDigit(value);
}
