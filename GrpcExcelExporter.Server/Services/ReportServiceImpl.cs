using Grpc.Core;
using GrpcExcelExporter.Protos;
using Microsoft.Data.SqlClient;
using System.Data;

namespace GrpcExcelExporter.Server.Services;

public class ReportServiceImpl : ReportService.ReportServiceBase
{
    private readonly string _connectionString;

    public ReportServiceImpl(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration));
    }

    public override async Task StreamAuditLogs(
        ReportRequest request,
        IServerStreamWriter<AuditLogStreamResponse> responseStream,
        ServerCallContext context)
    {
        const string query = """
            SELECT TOP (@RecordCount) 
                Id, TransactionId, UserId, Amount, StatusCode, CreatedDate, Description 
            FROM AuditLogs WITH (NOLOCK)
            ORDER BY Id ASC
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(context.CancellationToken);

        await using var command = new SqlCommand(query, connection);
        command.Parameters.Add("@RecordCount", SqlDbType.Int).Value = request.RecordCount;

        // CommandBehavior.SequentialAccess: Veriyi LOH'a yüklemeden stream eder (Senior dokunuş)
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, context.CancellationToken);

        // Tek bir response nesnesi oluşturup döngüde güncelleyerek Heap Allocation'ı minimuma indiriyoruz
        var responseBuffer = new AuditLogStreamResponse();

        while (await reader.ReadAsync(context.CancellationToken))
        {
            responseBuffer.Id = reader.GetInt64(0);
            responseBuffer.TransactionId = reader.GetGuid(1).ToString();
            responseBuffer.UserId = reader.GetInt32(2);
            responseBuffer.Amount = (double)reader.GetDecimal(3);
            responseBuffer.StatusCode = reader.GetInt32(4);
            responseBuffer.CreatedDate = reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss.fff");
            responseBuffer.Description = reader.GetString(6);

            // Veriyi ağ soketine anında basar
            await responseStream.WriteAsync(responseBuffer);
        }
    }
}
