using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TestAPI;

namespace MainSpace
{
    class ConsoleProgram
    {

        static async Task Main()
        {
            while (true)
            {
                Console.WriteLine();
                Console.WriteLine("1. List all files");
                Console.WriteLine("2. List latest file");
                Console.WriteLine("3. Download full version (latest)");
                Console.WriteLine("4. Download delta version (latest)");
                Console.WriteLine("5. Load folder to database");
                Console.WriteLine("6. Exit");
                Console.Write("Select option (1-6): ");

                if (!int.TryParse(Console.ReadLine(), out int choice))
                {
                    Console.WriteLine("Invalid input, enter a number");
                    continue;
                }

                switch (choice)
                {
                    case 1:
                        Console.Clear();
                        await API.ExecuteWithRetry(API.ListAllFiles);
                        break;
                    case 2:
                        Console.Clear();
                        await API.ExecuteWithRetry(API.ShowLatestFile);
                        break;
                    case 3:
                        Console.Clear();
                        await API.ExecuteWithRetry(API.DownloadFullVersion);
                        break;
                    case 4:
                        Console.Clear();
                        await API.ExecuteWithRetry(API.DownloadDeltaVersion);
                        break;
                    case 5:
                        Console.Clear();
                        await API.ExecuteWithRetry(API.LoadFolderToDatabase);
                        break;
                    case 6:
                        Console.WriteLine("\nstopping..");
                        return;
                    default:
                        Console.WriteLine("select 1-6.");
                        break;
                }
            }
        }
    }
}