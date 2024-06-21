using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SAPHanaAPI.Services
{
    public class SynchronizationService : IHostedService, IDisposable
    {
        private readonly ILogger<SynchronizationService> _logger;
        private readonly SapHanaService _sapHanaService;
        private Timer _timer;

        public SynchronizationService(ILogger<SynchronizationService> logger, SapHanaService sapHanaService)
        {
            _logger = logger;
            _sapHanaService = sapHanaService;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Synchronization Service is starting.");
            _timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromMinutes(5)); // Polling cada 5 minutos
            return Task.CompletedTask;
        }

        private void DoWork(object state)
        {
            _logger.LogInformation("Synchronization Service is working.");
            _sapHanaService.SyncClients().Wait(); // Sincronizar clientes
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Synchronization Service is stopping.");
            _timer?.Change(Timeout.Infinite, 0);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
