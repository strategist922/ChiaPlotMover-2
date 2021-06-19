using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace ChiaPlotMover
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            var configuration = new ConfigurationBuilder()
                .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                .AddJsonFile("app-settings.json", optional: false, reloadOnChange: true)
                .Build();
            serviceCollection.Configure<ChiaPlotConfiguration>(configuration.GetSection("ChiaPlotConfiguration"));
            serviceCollection.AddSingleton<IConfiguration>(configuration);
            var serviceProvider = serviceCollection.BuildServiceProvider(); 

            var config = serviceProvider.GetRequiredService<IOptions<ChiaPlotConfiguration>>().Value;

            var startTime = DateTime.Now;
            var totalFilesTransfered = 0;
            long totalBytesTransfered = 0;
            var destinationIndex = 0;
            var maxParallelTransfers = 2;
            while(true) 
            {
                var tasks = new List<Task>();
                foreach (var folder in config.PlotLocations) 
                {
                    var plots = Directory.GetFiles(folder, "*.plot").Where(f => f.IndexOf(".plot.") == -1).Take(1);

                    foreach(var plot in plots) 
                    {
                        if (tasks.Count >= maxParallelTransfers) {
                            await Task.WhenAll(tasks.ToArray());
                            tasks.Clear();
                        }
                        var fileName = Path.GetFileName(plot);
                        
                        var fileInfo = new FileInfo(plot);
                        while(true)
                        {
                            if (destinationIndex == config.DestinationDrives.Count) {
                                destinationIndex = 0;
                            }
                            var destination = config.DestinationDrives[destinationIndex];
                            destinationIndex++;
                            var driveInfo = new DriveInfo(destination);

                            try
                            {
                                if (driveInfo.AvailableFreeSpace > fileInfo.Length) 
                                {
                                    tasks.Add(Task.Run(() => {
                                        try
                                        {
                                            
                                            Console.WriteLine($"Moving {plot} to {destination}");
                                            var fileSize = fileInfo.Length;
                                            File.Move(plot, Path.Combine(destination, fileName), true);
                                            Console.WriteLine("Done moving plot... moving next plot");
                                            totalFilesTransfered++;
                                            totalBytesTransfered += fileSize;
                                        }
                                        catch(Exception ex)
                                        {
                                            Console.WriteLine($"{DateTime.Now.ToString()}");
                                            Console.WriteLine($"Exception Dump: {ex}");
                                            Console.WriteLine($"InnerException: {ex.InnerException}");
                                        }
                                    }));
                                    
                                    break;
                                }
                            }
                            catch(Exception ex) 
                            {
                                Console.WriteLine($"Unexpected Exception: {ex.Message}");
                                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                            }
                        }
                    }
                }
                if (tasks.Count > 0) 
                {
                    await Task.WhenAll(tasks.ToArray());
                    tasks.Clear();
                    Console.WriteLine($"Transfered {totalFilesTransfered} since {startTime.ToString()}");
                    var diff = DateTime.Now.Subtract(startTime).TotalHours;
                    Console.WriteLine($"{(totalBytesTransfered / diff) / 1000000000000} TiB/hour");
                }
            
                Console.WriteLine("Holding 2 minutes...");
                System.Threading.Thread.Sleep(2 * 60 * 1000);
            }
        }
    }
}
