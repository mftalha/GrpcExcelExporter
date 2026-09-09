using Grpc.Core;
using Grpc.Net.Client;
using GrpcExcelExporter.Protos;
using System.Diagnostics;
using System.Text;

Console.WriteLine("=== gRPC High-Throughput Stream Client ===");
Console.WriteLine("5 Milyon Satırlık Veri Akışı Başlatılıyor...\n");

// HTTP/2 bağlantısı üzerinden gRPC kanalı
using var channel = GrpcChannel.ForAddress("https://localhost:7251");
var client = new ReportService.ReportServiceClient(channel);

var request = new ReportRequest { RecordCount = 5000000 };

var stopwatch = Stopwatch.StartNew();
var outputFilePath = Path.Combine(Directory.GetCurrentDirectory(), "AuditLogs_Export.csv");

// Disk I/O darboğazını önlemek için 64 KB'lık BufferedStream kullanımı
await using var fileStream = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
await using var writer = new StreamWriter(fileStream, Encoding.UTF8);

// CSV Header
await writer.WriteLineAsync("Id,TransactionId,UserId,Amount,StatusCode,CreatedDate,Description");

using var call = client.StreamAuditLogs(request);
int processedRows = 0;

// IAsyncEnumerable mantığı ile veriyi geldikçe diske yazıyoruz (Memory $O(1)$)
await foreach (var row in call.ResponseStream.ReadAllAsync())
{
    await writer.WriteLineAsync($"{row.Id},{row.TransactionId},{row.UserId},{row.Amount},{row.StatusCode},{row.CreatedDate},\"{row.Description}\"");

    processedRows++;

    if (processedRows % 500000 == 0)
    {
        Console.WriteLine($"[{stopwatch.Elapsed.TotalSeconds:F2}s] Aktarılan Satır: {processedRows:N0}");
    }
}

stopwatch.Stop();

Console.WriteLine("\n=== İşlem Tamamlandı ===");
Console.WriteLine($"Toplam İşlenen Satır : {processedRows:N0}");
Console.WriteLine($"Toplam Süre          : {stopwatch.Elapsed.TotalSeconds:F2} saniye");
Console.WriteLine($"Ortalama Hız         : {processedRows / stopwatch.Elapsed.TotalSeconds:N0} satır/sn");
Console.WriteLine($"Oluşturulan Dosya    : {outputFilePath}");


// Konsolun kapanmasını önlemek için:
Console.WriteLine("\nÇıkmak için bir tuşa basın...");
Console.ReadKey();