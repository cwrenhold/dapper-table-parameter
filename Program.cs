using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using Dapper;
using Microsoft.SqlServer.Server;

// Load the .env file for the environment variables
DotNetEnv.Env.Load();
var connectionString = Environment.GetEnvironmentVariable("CONNECTION_STRING");

using var connection = new SqlConnection(connectionString);

var query = @"
SELECT
    r.Id
,   r.Text
,   r.UserId
FROM Record r
INNER JOIN @UserIds AS udt ON udt.Id = r.UserId
";

var userIds = Enumerable.Range(0, 5000).ToArray();

// Using Data Table and function
// var records = connection.Query<Record>(query, new { UserIds = convertFromIntsToUdtIntId(userIds) });

// Using uddtIntId
// var records = connection.Query<Record>(query, new UdtIntId("@UserIds", userIds));

// Using udttIntId with DataTable
var records = connection.Query<Record>(query, new { UserIds = new UdtIntIdWithDataTable(userIds) { ConvertToTempTable = false } });

Console.WriteLine($"Id\tText\tUserId");
foreach (var record in records)
{
    Console.WriteLine($"{record.Id}\t{record.Text}\t{record.UserId}");
}

DataTable convertFromIntsToDataTable(int[] ints)
{
    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(int));
    foreach (var id in ints)
    {
        dt.Rows.Add(id);
    }
    return dt;
}

SqlMapper.ICustomQueryParameter convertFromIntsToUdtIntId(int[] ints)
{
    return convertFromIntsToDataTable(ints).AsTableValuedParameter("dbo.udttIntId");
}

class Record
{
    public int Id { get; set; }
    public string Text { get; set; } = string.Empty;
    public int UserId { get; set; }
}

class UdtIntIdWithDataTable : SqlMapper.ICustomQueryParameter
{
    public UdtIntIdWithDataTable(params int[] ids)
    {
        Ids = ids;
    }

    public int[] Ids { get; }

    public bool ConvertToTempTable { get; set; } = true;

    public void AddParameter(IDbCommand command, string name)
    {
        var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        foreach (var id in this.Ids)
        {
            dt.Rows.Add(id);
        }

        var sqlCommand = (SqlCommand)command;

        var parameter = dt.AsTableValuedParameter("dbo.udttIntId");

        if (ConvertToTempTable)
        {
            sqlCommand.CommandText = sqlCommand.CommandText.Replace($"@{name}", $"#{name}");

            // Update the query to convert the table variable to a temp table
            sqlCommand.CommandText = $@"
                DROP TABLE IF EXISTS #{name}

                SELECT *
                INTO #{name}
                FROM @{name}

            " + sqlCommand.CommandText;
        }

        parameter.AddParameter(sqlCommand, name);
    }
}

class UdtIntId : Dapper.SqlMapper.IDynamicParameters
{
    public UdtIntId(string parameterName, params int[] ids)
    {
        ParameterName = parameterName;
        Ids = ids;
    }

    public string ParameterName { get; }

    public int[] Ids { get; }

    public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
    {
        var sqlCommand = (SqlCommand)command;

        var idList = new List<SqlDataRecord>();

        var typeDefinition = new SqlMetaData[] { new SqlMetaData("Id", SqlDbType.Int) };

        foreach (var id in Ids)
        {
            var record = new SqlDataRecord(typeDefinition);
            record.SetInt32(0, id);
            idList.Add(record);
        }

        var parameter = sqlCommand.Parameters.Add(this.ParameterName, SqlDbType.Structured);
        parameter.Direction = ParameterDirection.Input;
        parameter.TypeName = "dbo.udttIntId";
        parameter.Value = idList;
    }
}
