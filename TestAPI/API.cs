using System.Data;
using System.IO.Compression;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Npgsql;

namespace TestAPI
{
    public static class API
    {
        #region Для заполнения/изменения
        const string url = "https://fias.nalog.ru/WebServices/Public/GetAllDownloadFileInfo";
        const string DownloadPath = @"C:\TestDownload\";
        private static string _currentConnectionString = "Host=localhost;Port=5432;Database=TestAPI;Username=postgres;Password=admin;";
        #endregion


        // Весь вывод у меня на английском т.к. консоль не обрабатывает русский и я хз как это фиксить :b
        public static async Task<List<FiasFileInfo>> GetFiasFilesInfoAsync(string url)
        {
            using var httpClient = new HttpClient();

            httpClient.DefaultRequestHeaders.Add("User-Agent", "TestFiasClient/1.0 (Windows 11; Win64; x64)"); // Может тоже нужно поменять, хотя-бы название, не уверен насколько сайт это проверяет
            httpClient.Timeout = TimeSpan.FromMinutes(6);

            var response = await httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<FiasFileInfo>>(json);
        }

        public static async Task ExecuteWithRetry(Func<Task> operation)
        {
            while (true)
            {
                try
                {
                    await operation();
                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"error: {ex.Message}");
                    Console.Write("Retry? (y/n): ");
                    var response = Console.ReadLine()?.Trim();

                    if (response != "y")
                    {
                        Console.WriteLine("sure\n");
                        return;
                    }
                    Console.WriteLine("retrying..\n");
                }
            }
        }

        public static async Task ListAllFiles()
        {
            var files = await FetchFiles(); files.Reverse();
            Console.WriteLine($"there is total of {files.Count} versions:");

            foreach (var file in files)
            {
                PrintFileInfo(file);
            }
        }

        public static async Task ShowLatestFile()
        {
            var files = await FetchFiles();
            var latest = files.First();

            Console.WriteLine("last version:");
            PrintFileInfo(latest);
        }

        public static async Task DownloadFullVersion()
        {
            var files = await FetchFiles();
            var latest = files.First();

            Console.WriteLine($"Downloading full: {latest.GarXMLFullURL}");
            await DownloadFile(latest.GarXMLFullURL, "full_gar_xml.zip");

            Console.WriteLine("\nany key to return to menu...");
            Console.ReadKey(true);
            Console.Clear();
        }

        public static async Task DownloadDeltaVersion()
        {
            var files = await FetchFiles();
            var latest = files.First();

            Console.WriteLine($"Downloading delta: {latest.GarXMLDeltaURL}");
            await DownloadFile(latest.GarXMLDeltaURL, "delta_gar_xml.zip");

            Console.WriteLine("\nany key to return to menu...");
            Console.ReadKey(true);
            Console.Clear();
        }

        static async Task DownloadFile(string url, string fileName)
        {
            Directory.CreateDirectory(DownloadPath);
            string fullPath = Path.Combine(DownloadPath, fileName);

            using var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Add("User-Agent", "TestFiasClient/1.0 (Windows 11; Win64; x64)");
            httpClient.Timeout = TimeSpan.FromHours(6);

            try
            {
                using (var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead))
                {
                    response.EnsureSuccessStatusCode();

                    using (var contentStream = await response.Content.ReadAsStreamAsync())
                    using (var fileStream = new FileStream(fullPath, FileMode.Create, FileAccess.Write))
                    {
                        var totalBytes = response.Content.Headers.ContentLength;
                        var buffer = new byte[8192];
                        var totalRead = 0L;
                        var bytesRead = 0;
                        var lastUpdate = DateTime.Now;

                        while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                        {
                            await fileStream.WriteAsync(buffer, 0, bytesRead);
                            totalRead += bytesRead;

                            if (DateTime.Now - lastUpdate > TimeSpan.FromMilliseconds(200) || totalRead == totalBytes)
                            {
                                if (totalBytes.HasValue)
                                {
                                    var progress = (double)totalRead / totalBytes.Value * 100;
                                    Console.Write($"\rDownload progress: {progress:0.0}% ({totalRead / 1024 / 1024}/{totalBytes / 1024 / 1024} MB)");
                                }
                                else
                                {
                                    Console.Write($"\rDownloaded: {totalRead} MB");
                                }
                                lastUpdate = DateTime.Now;
                            }
                        }
                        Console.WriteLine($"\nDownloaded to: {fullPath}");
                    }
                }
            }
            catch (Exception ex)
            {
                if (File.Exists(fullPath))
                {
                    File.Delete(fullPath);
                }
                throw new Exception($"Download failed: {ex.Message}");
            }
        }

        static async Task<List<FiasFileInfo>> FetchFiles()
        {
            try
            {
                var files = await TestAPI.API.GetFiasFilesInfoAsync(url);
                if (files == null || files.Count == 0)
                {
                    throw new Exception("no files received from server");
                }
                return files;
            }
            catch
            {
                throw new Exception("server not responding");
            }
        }

        static void PrintFileInfo(FiasFileInfo file)
        {
            Console.WriteLine($"\nVersion: {file.VersionId} ({file.TextVersion})");
            Console.WriteLine($"Date: {file.Date}");
            Console.WriteLine($"Full XML: {file.GarXMLFullURL}");
            Console.WriteLine($"Delta XML: {file.GarXMLDeltaURL}");
        }

        public static async Task LoadFolderToDatabase()
        {
            string extractPath = "";
            try
            {
                string zipPath = DownloadPath + "gar_xml.zip";

                Console.Write("Enter folder number (1-99): ");
                if (!int.TryParse(Console.ReadLine(), out int folderNumber) || folderNumber < 1 || folderNumber > 99)
                {
                    Console.WriteLine("Invalid folder number!");
                    return;
                }

                string formattedFolderName = folderNumber.ToString("00");

                extractPath = Path.Combine(Path.GetTempPath(), $"tapi_temp_extract_{Guid.NewGuid()}");
                Directory.CreateDirectory(extractPath);

                Console.WriteLine($"Extracting folder {formattedFolderName} from archive...");
                await ExtractSingleFolder(zipPath, formattedFolderName, extractPath);

                var xmlFiles = Directory.GetFiles(extractPath, "*.xml");
                if (xmlFiles.Length == 0)
                {
                    Console.WriteLine($"No XML files found in folder {formattedFolderName}!");
                    return;
                }

                Console.WriteLine($"Found {xmlFiles.Length} files, Processing...");

                foreach (var xmlFile in xmlFiles)
                {
                    Console.WriteLine($"Processing {Path.GetFileName(xmlFile)}");
                    await ProcessXmlFile(xmlFile);
                }

                Console.WriteLine("All files processed successfully!");
            }
            catch (Exception ex)
            {
                throw new Exception($"Database load failed: {ex.Message}");
            }
            finally
            {
                if (Directory.Exists(extractPath))
                {
                    Directory.Delete(extractPath, true);
                    Console.WriteLine("Temporary files cleaned up.");
                }
            }
        }
        private static async Task ExtractSingleFolder(string zipPath, string folderName, string extractPath)
        {
            using (var archive = ZipFile.OpenRead(zipPath))
            {
                bool folderFound = false;
                string folderPrefix = folderName + "/";

                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.StartsWith(folderPrefix, StringComparison.OrdinalIgnoreCase) &&
                        !string.IsNullOrEmpty(entry.Name))
                    {
                        folderFound = true;
                        string destinationPath = Path.Combine(extractPath, entry.Name);

                        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath));

                        entry.ExtractToFile(destinationPath, overwrite: true);
                        Console.WriteLine($"Extracted: {entry.FullName}");
                    }
                }

                if (!folderFound)
                {
                    throw new Exception($"Folder '{folderName}' not found in archive!");
                }
            }
        }

        private static async Task ProcessXmlFile(string xmlPath)
        {
            try
            {
                Console.WriteLine($"Processing XML: {Path.GetFileName(xmlPath)}");

                string fileName = Path.GetFileNameWithoutExtension(xmlPath);

                string[] parts = fileName.Split('_');

                string tableName = parts.Length > 2
                    ? string.Join("_", parts.Take(parts.Length - 2))
                    : fileName;

                Console.WriteLine($"Using table name: {tableName}");

                XDocument doc = XDocument.Load(xmlPath);
                XElement root = doc.Root;

                var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (root.Elements().FirstOrDefault() is XElement firstElement)
                {
                    foreach (XAttribute attr in firstElement.Attributes())
                    {
                        columns.Add(attr.Name.LocalName);
                    }
                }

                Console.WriteLine($"Found {columns.Count} columns: {string.Join(", ", columns)}");

                await CreateTable(tableName, columns);

                await BulkInsertData(tableName, columns, root.Elements());

                Console.WriteLine($"Successfully processed {root.Elements().Count()} records");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing XML: {ex.Message}");
                throw;
            }
        }
        private static async Task CreateTable(string tableName, HashSet<string> columns)
        {
            using (var conn = new NpgsqlConnection(_currentConnectionString))
            {
                await conn.OpenAsync();

                var createTableCmd = new NpgsqlCommand(
                    $@"CREATE TABLE IF NOT EXISTS ""{tableName}"" ()",
                    conn);

                await createTableCmd.ExecuteNonQueryAsync();
                Console.WriteLine($"Table {tableName} created or exists");

                var existingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var getColumnsCmd = new NpgsqlCommand(
                    $@"SELECT column_name 
               FROM information_schema.columns 
               WHERE table_name = '{tableName.ToLower()}'",
                    conn);

                using (var reader = await getColumnsCmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        existingColumns.Add(reader.GetString(0));
                    }
                }

                foreach (var column in columns)
                {
                    if (!existingColumns.Contains(column))
                    {
                        var addColumnCmd = new NpgsqlCommand(
                            $@"ALTER TABLE ""{tableName}"" ADD COLUMN ""{column}"" TEXT",
                            conn);

                        try
                        {
                            await addColumnCmd.ExecuteNonQueryAsync();
                            Console.WriteLine($"Added column: {column}");
                        }
                        catch (PostgresException ex) when (ex.SqlState == "42701")
                        {
                            Console.WriteLine($"Column {column} already exists");
                        }
                    }
                }
            }
        }

        private static async Task BulkInsertData(string tableName, HashSet<string> columns, IEnumerable<XElement> elements)
        {
            try
            {
                if (columns == null || columns.Count == 0)
                {
                    Console.WriteLine($"Skipping {tableName} - no columns found");
                    return;
                }

                using (var conn = new NpgsqlConnection(_currentConnectionString))
                {
                    await conn.OpenAsync();

                    var columnList = string.Join(", ", columns.Select(c => $@"""{c}"""));
                    var copyCommand = $@"COPY ""{tableName}"" ({columnList}) FROM STDIN (FORMAT BINARY)";

                    Console.WriteLine($"Starting COPY for {tableName}...");

                    using (var writer = conn.BeginBinaryImport(copyCommand))
                    {
                        int rowCount = 0;
                        var columnArray = columns.ToArray();

                        foreach (var element in elements)
                        {
                            writer.StartRow();

                            for (int i = 0; i < columnArray.Length; i++)
                            {
                                var attr = element.Attribute(columnArray[i]);
                                writer.Write(attr?.Value ?? (object)DBNull.Value);
                            }

                            rowCount++;

                            if (rowCount % 10000 == 0)
                            {
                                Console.Write($"\rCopied {rowCount} rows...");
                            }
                        }

                        await writer.CompleteAsync();
                        Console.WriteLine($"\nSuccessfully inserted {rowCount} rows into {tableName}");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Bulk insert failed for table {tableName}: {ex.Message}");
            }
        }
        public class FiasFileInfo
        {
            [JsonProperty("VersionId")]
            public int VersionId { get; set; }

            [JsonProperty("TextVersion")]
            public string TextVersion { get; set; }

            [JsonProperty("FiasCompleteDbfUrl")]
            public string FiasCompleteDbfUrl { get; set; }

            [JsonProperty("FiasCompleteXmlUrl")]
            public string FiasCompleteXmlUrl { get; set; }

            [JsonProperty("FiasDeltaDbfUrl")]
            public string FiasDeltaDbfUrl { get; set; }

            [JsonProperty("FiasDeltaXmlUrl")]
            public string FiasDeltaXmlUrl { get; set; }

            [JsonProperty("Kladr4ArjUrl")]
            public string Kladr4ArjUrl { get; set; }

            [JsonProperty("Kladr47ZUrl")]
            public string Kladr47ZUrl { get; set; }

            [JsonProperty("GarXMLFullURL")]
            public string GarXMLFullURL { get; set; }

            [JsonProperty("GarXMLDeltaURL")]
            public string GarXMLDeltaURL { get; set; }

            [JsonProperty("ExpDate")]
            public DateTime ExpDate { get; set; }

            [JsonProperty("Date")]
            public string Date { get; set; }
        }
    }
}