using GrpcExcelExporter.Server.Services;

//var builder = WebApplication.CreateBuilder(args);
var builder = WebApplication.CreateSlimBuilder(args); // Native AOT uyumlu builder

// SlimBuilder varsayılan olarak HTTPS servislerini yüklemez.
// Kestrel'in HTTPS/TLS ayarlarını otomatik okuyabilmesi için bu satırı ekliyoruz:
builder.WebHost.UseKestrelHttpsConfiguration();

// Add services to the container.
// gRPC servislerini IoC'ye ekle
builder.Services.AddGrpc();

var app = builder.Build();

// Configure the HTTP request pipeline.
//app.MapGrpcService<GreeterService>();
//app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

// Endpoint'i dışa aç
app.MapGrpcService<ReportServiceImpl>();

app.Run();
