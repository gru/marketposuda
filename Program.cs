using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using FluentValidation;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace MarketPosuda
{
    public class Program
    {
        private static Logger _logger;
        
        public static int Main(string[] args)
        {
            try
            {
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(Config.ProcessDirectory)
                    .AddJsonFile("appsettings.json")
                    .AddCommandLine(args)
                    .Build();

                _logger = new LoggerConfiguration()
                    .MinimumLevel.Verbose()
                    .WriteTo.Console(LogEventLevel.Information)
                    .WriteTo.File(Path.Combine(Config.ProcessDirectory, "лог.txt") , LogEventLevel.Debug)
                    .CreateLogger();

                _logger.Debug("Текущее время {date}", DateTimeOffset.Now);
                _logger.Debug(configuration.GetDebugView());

                var config = configuration
                    .Get<Config>();

                if (string.IsNullOrWhiteSpace(config.Source))
                    config.Source = ReadLine("Файл с текущими значениями: ");

                if (string.IsNullOrWhiteSpace(config.Input))
                    config.Input = ReadLine("Файл с новыми значениями: ");

                if (string.IsNullOrWhiteSpace(config.Output))
                    config.Output = $"{Path.GetFileNameWithoutExtension(config.Source)}.{DateTimeOffset.Now:yyyyMMddHHmmss}{Path.GetExtension(config.Source)}";

                _logger.Information("Файл с текущими значениями: {source}", config.Source);
                _logger.Information("Файл с новыми значениями: {input}", config.Input);
                _logger.Information("Выходной файл: {output}", config.Output);

                var validator = new ConfigValidator();
                var validationResult = validator.Validate(config);
                if (validationResult.IsValid)
                {
                    var source = ParseFile(config.SourcePath, config.SourceDelimiter, config.SourceEncoding)
                        .ToArray();
                    var input = ParseFile(config.InputPath, config.InputDelimiter, config.InputEncoding)
                        .ToDictionary(r => r[config.InputMatchBy], r => r, StringComparer.OrdinalIgnoreCase);

                    foreach (var row in source)
                    {
                        var sourceValue = row[config.SourceMatchBy];
                        if (input.TryGetValue(sourceValue, out var inputRow))
                        {
                            _logger.Information($"Обновляем значение для '{sourceValue}' с {row[config.SourceField]} на {inputRow[config.InputField]}");

                            row[config.SourceField] = inputRow[config.InputField];
                        }
                        else
                        {
                            _logger.Debug("Не найдена запись {sourceValue} в файле с новыми значениями", sourceValue);
                        }
                    }

                    _logger.Debug("Записываем данные в {outputPath}", config.OutputPath);

                    var outputCsvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
                    {
                        Delimiter = config.OutputDelimiter
                    };
                    
                    using (var file = new FileStream(config.OutputPath, FileMode.Create, FileAccess.Write))
                    using (var writer = new StreamWriter(file, config.OutputEncoding))
                    using (var csv = new CsvWriter(writer, outputCsvConfiguration))
                    {
                        foreach (var row in source)
                        {
                            _logger.Debug("Строка: {row}", string.Join(config.OutputDelimiter, row.Take(3)));

                            foreach (var field in row)
                            {
                                csv.WriteField(field);
                            }

                            csv.NextRecord();
                        }
                    }
                }
                else
                {
                    foreach (var error in validationResult.Errors)
                        _logger.Error(error.ErrorMessage);
                }
                
                _logger.Debug("Текущее время {date}", DateTimeOffset.Now);
            }
            catch (Exception ex)
            {
                _logger?.Error("Необработанная ошибка в приложении:", ex);
                
                Console.WriteLine(ex);

                return -1;
            }

            Console.WriteLine("Готово. Нажмите любую клавишу...");
            Console.ReadLine();

            return 0;
        }

        private static IEnumerable<string[]> ParseFile(string path, string delimiter, Encoding encoding)
        {
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter, 
                BadDataFound = null
            };
            
            using (var stream = new FileStream(path, FileMode.Open))
            using (var reader = new StreamReader(stream, encoding))
            using (var csvReader = new CsvReader(reader, configuration))
            {
                csvReader.Read();
                csvReader.ReadHeader();

                _logger.Debug("Заголовок: {header}", string.Join(delimiter, csvReader.Context.HeaderRecord));
                
                yield return csvReader.Context.HeaderRecord;

                while (csvReader.Read())
                {
                    _logger.Debug("Строка {row}: {data}", csvReader.Context.Row, string.Join(delimiter, csvReader.Context.Record.Take(3)));
                    
                    yield return csvReader.Context.Record;
                }
            }
        }

        private static string ReadLine(string text)
        {
            Console.WriteLine(text);
            
            return Console.ReadLine();
        }
    }

    internal class Config
    {
        private static readonly Lazy<string> _processDirectory;

        static Config()
        {
            _processDirectory = new Lazy<string>(GetProcessDirectory);
        }
        
        public Config()
        {
            InputDelimiter = ";";
            OutputDelimiter = ";";
            SourceDelimiter = ",";
            SourceEncodingName = "utf-8";
            OutputEncodingName = "utf-8";
            InputEncodingName = "utf-8";
            SourceMatchBy = 2;
            SourceField = 10;
            InputMatchBy = 0;
            InputField = 4;
        }
        
        public string Input { get; set; }

        public string Output { get; set; }
        
        public string Source { get; set; }
        
        public string InputDelimiter { get; set; }

        public string OutputDelimiter { get; set; }
        
        public string SourceDelimiter { get; set; }

        public int InputField { get; set; }
        
        public int SourceField { get; set; }
        
        public int InputMatchBy { get; set; }
        
        public int SourceMatchBy { get; set; }
        
        public string SourceEncodingName { get; set; }
        
        public string OutputEncodingName { get; set; }
        
        public string InputEncodingName { get; set; }

        public string InputPath
            => Path.Combine(ProcessDirectory, Input);

        public string OutputPath
            => Path.Combine(ProcessDirectory, Output);
        
        public string SourcePath
        => Path.Combine(ProcessDirectory, Source);
        
        public Encoding SourceEncoding
            => Encoding.GetEncoding(SourceEncodingName);
        
        public Encoding OutputEncoding
            => Encoding.GetEncoding(OutputEncodingName);
        
        public Encoding InputEncoding
            => Encoding.GetEncoding(InputEncodingName);

        public static string ProcessDirectory
            => _processDirectory.Value;
        
        // https://github.com/dotnet/runtime/issues/40862
        private static string GetProcessDirectory()
        {
            var mainModule = Process.GetCurrentProcess().MainModule;
            if (mainModule == null)
                throw new Exception("Не найден MainModule процесса");
            
            var debugPath = Path.GetDirectoryName(Environment.GetCommandLineArgs()[0]);
            var execPath = Path.GetDirectoryName(mainModule.FileName);

            if (debugPath != null && File.Exists(Path.Combine(debugPath, "appsettings.json")))
                return debugPath;
            
            if (execPath != null && File.Exists(Path.Combine(execPath, "appsettings.json")))
                return execPath;
            
            throw new Exception("Не удалось определить расположение программы");
        }
    }

    internal class ConfigValidator : AbstractValidator<Config>
    {
        public ConfigValidator()
        {
            RuleFor(c => c.Input)
                .NotEmpty()
                .Must(FileExist)
                .WithMessage("Не указан или не найден файл с текущими значениями");
            
            RuleFor(c => c.Output)
                .NotEmpty()
                .WithMessage("Не указан или не найден выходной файл");
            
            RuleFor(c => c.Source)
                .NotEmpty()
                .Must(FileExist)
                .WithMessage("Не указан или не найден файл с новыми значениями");

            RuleFor(c => c.InputEncodingName)
                .NotEmpty()
                .Must(EncodingExist)
                .WithMessage(c => $"Не указана или не найдена кодировка {c.InputEncodingName} файла с текущими значениями");
            
            RuleFor(c => c.OutputEncodingName)
                .NotEmpty()
                .Must(EncodingExist)
                .WithMessage(c => $"Не указана или не найдена кодировка {c.OutputEncodingName} выходного файла");
            
            RuleFor(c => c.SourceEncodingName)
                .NotEmpty()
                .Must(EncodingExist)
                .WithMessage(c => $"Не указана или не найдена кодировка {c.SourceEncodingName} с новыми значениями");
        }

        private static bool EncodingExist(string name)
        {
            try
            {
                Encoding.GetEncoding(name);

                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool FileExist(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;
            
            return File.Exists(Path.Combine(Config.ProcessDirectory, name));
        }
    }
}