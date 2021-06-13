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
            // var directoryInfo = new DriveInfo("/chia/plots/share/208");
            // Console.WriteLine(directoryInfo.AvailableFreeSpace);
            //
            // File.Move("/chia/plottemp2/plot-k32-2021-06-03-17-18-9480cd2c62bcd8f1f81218592995ffe99334185e3cb2ca0947d2a29b6b451789.plot.2.tmp", "/chia/plots/share/208/plot-k32-2021-06-03-17-18-9480cd2c62bcd8f1f81218592995ffe99334185e3cb2ca0947d2a29b6b451789.plot", true);
            // var plotTempFolders = new List<string> { "/chia/plottemp1", "/chia/plottemp2", "/chia/plottemp3", "/chia/plottemp4"};
            Console.WriteLine($"Start time - {DateTime.Now}");
            var destinationIndex = 0;
            while(true) 
            {
            // var destination = "/chia/plots/share/208";
            // var destinations = new List<string>() 
            //     { 
            //         // "/chia/plots/203",
            //         // "/chia/plots/204",
            //         // "/chia/plots/206",
            //         // "/chia/plots/207",
            //         "/chia/plots/share/100",
            //         "/chia/plots/share/101",
            //         "/chia/plots/share/102",
            //         "/chia/plots/share/103",
            //         "/chia/plots/share/104",
            //         "/chia/plots/share/105",
            //         "/chia/plots/share/106",
            //         "/chia/plots/share/107",
            //         "/chia/plots/share/108",
            //         "/chia/plots/share/109",
            //         "/chia/plots/share/200",
            //         "/chia/plots/share/201",
            //         "/chia/plots/share/202",
            //         "/chia/plots/share/203",
            //         "/chia/plots/share/204",
            //         "/chia/plots/share/205",
            //         "/chia/plots/share/206",
            //         "/chia/plots/share/207",
            //         "/chia/plots/share/208",
            //         "/chia/plots/share/209"
            //     };
            var maxParallelTransfers = 4;
            var tasks = new List<Task>();
            foreach (var folder in config.PlotLocations) 
            {
                var plots = Directory.GetFiles(folder, "*.plot").Where(f => f.IndexOf(".plot.") == -1);

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
                                        File.Move(plot, Path.Combine(destination, fileName), true);
                                        Console.WriteLine("Done moving plot... moving next plot");
                                        totalFilesTransfered++;
                                    }
                                    catch(Exception ex)
                                    {
                                        Console.WriteLine($"Skipping plot: {fileName}.  Exception: {ex.Message}");
                                    }
                                }));
                                
                                break;
                            }
                        }
                        catch(Exception ex) 
                        {
                            Console.WriteLine($"Ex: {ex.Message}");
                        }
                    }
                }
            }
            await Task.WhenAll(tasks.ToArray());
            tasks.Clear();
            Console.WriteLine($"Transfered {totalFilesTransfered} since {startTime.ToString()}");
            Console.WriteLine("Holding 5 minutes...");
            System.Threading.Thread.Sleep(5 * 60 * 1000);
            }
        }
    }
}
