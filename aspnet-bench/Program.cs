var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders(); 
var app = builder.Build();
app.MapStaticAssets();

app.Run();
