using Grpc.Core;
using GrpcExcelExporter.Protos;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;

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
    IServerStreamWriter<AuditLogBatchResponse> responseStream,
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

        // burda  CommandBehavior.SequentialAccess kullanıyoruz çünkü veriyi LOH'a yüklemeden stream ediyoruz
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, context.CancellationToken);

        var batch = new AuditLogBatchResponse();
        const int batchSize = 1000;
        const int timeoutMs = 500; // 500 milisaniye (0.5 saniye)
        batch.Items.Capacity = batchSize; // İlk genişleme kopyalamalarını da engeller
        

        // Zamanlayıcıyı başlatıyoruz (Allocation gerektirmez, struct tabanlıdır ve çok hızlıdır)
        var timer = Stopwatch.StartNew();

        while (await reader.ReadAsync(context.CancellationToken))
        {
            batch.Items.Add(new AuditLogStreamResponse
            {
                Id = reader.GetInt64(0),
                TransactionId = reader.GetGuid(1).ToString(),
                UserId = reader.GetInt32(2),
                Amount = (double)reader.GetDecimal(3),
                StatusCode = reader.GetInt32(4),
                CreatedDate = reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss.fff"),
                Description = reader.GetString(6)
                // içerik NVARCHAR(MAX) veya VARCHAR(MAX) osla idi ve ~40.000 karakteri (~85 KB) i geçme durumu olsa idi alttaki gibi kullanmalıydık:
                // Description = reader.GetTextReader(6) // eğerki 85 kb den az ilse loh a düşmüyorsa buna gerek yok gereksiz masraf o zaman.
            });

            // MİMARİ DOKUNUŞ: Limit 1000'e ulaştıysa VEYA 500ms süre dolduysa paketi yolla!
            if (batch.Items.Count >= batchSize || timer.ElapsedMilliseconds >= timeoutMs)
            {
                await responseStream.WriteAsync(batch);
                batch.Items.Clear();
                timer.Restart(); // Paketi gönderdikten sonra zamanlayıcıyı sıfırla
            }
        }

        // Kalan son parçayı gönder
        if (batch.Items.Count > 0)
        {
            await responseStream.WriteAsync(batch);
        }
    }

    /*
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
    */
}
