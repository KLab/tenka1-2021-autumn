using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace api
{
    internal static class Program
    {
        const int EXIT_CODE_RESTART = 64;

        public static async Task Main(string[] args)
        {
            var calcScoreMode = true;
            var apiMode = true;
            var stagingMode = false;
            var batchMode = false;
            var initializeGameData = false;
            var masterDataGenerate = false;
            int? startOffset = null;
            int? periodArg = null;
            (long, int)? startAt = null;
            if (args.Length >= 1)
            {
                switch (args[0])
                {
                    case "calcScore":
                        apiMode = false;
                        break;
                    case "api":
                        calcScoreMode = false;
                        break;
                    case "apiStaging":
                        calcScoreMode = false;
                        stagingMode = true;
                        break;
                    case "batch":
                        apiMode = false;
                        calcScoreMode = false;
                        batchMode = true;
                        break;
                    case "gen":
                        var path = Environment.GetEnvironmentVariable("OUTPUT_PATH");
                        Debug.Assert(path != null, nameof(path) + " != null");
                        var period = int.Parse(args[1]);
                        await System.IO.File.WriteAllBytesAsync(path, MasterDataGenerator.Generate(period));
                        return;
                    case "-":
                        break;
                    default:
                        throw new Exception("invalid args[0] (mode)");
                }
            }

            if (args.Length >= 2)
            {
                if (args[1].StartsWith("+"))
                {
                    var a = args[1].Substring(1).Split("/");
                    Debug.Assert(a.Length == 1 || a.Length == 2);
                    startOffset = int.Parse(a[0]);
                    if (a.Length == 2)
                    {
                        periodArg = int.Parse(a[1]);
                    }
                }
                else if (args[1].StartsWith("/"))
                {
                    var a = args[1].Substring(1).Split("/");
                    Debug.Assert(a.Length == 2);
                    startAt = (long.Parse(a[0]), int.Parse(a[1]));
                }
                else if (args[1] != "-")
                {
                    throw new Exception("invalid args[1] (startOffset)");
                }
            }

            if (args.Length >= 3)
            {
                switch (args[2])
                {
                    case "generate":
                        initializeGameData = true;
                        masterDataGenerate = true;
                        break;
                    case "-":
                        break;
                    default:
                        throw new Exception("invalid args[2] (masterDataMode)");
                }
            }

            var gameDbHost = Environment.GetEnvironmentVariable("GAMEDB_HOST") ?? "localhost";
            var gameDbPort = Environment.GetEnvironmentVariable("GAMEDB_PORT") ?? "6379";
            var redisConfig = ConfigurationOptions.Parse($"{gameDbHost}:{gameDbPort}");
            var redis = ConnectionMultiplexer.Connect(redisConfig);

            if (initializeGameData)
            {
                Api.InitializeGameData(redis, redisConfig);
            }

            if (periodArg.HasValue)
            {
                MasterDataGenerator.SetPeriod(redis, periodArg.Value);
            }

            if (startOffset.HasValue)
            {
                MasterDataGenerator.SetTime(redis, startOffset.Value);
            }

            if (startAt.HasValue)
            {
                MasterDataGenerator.SetTimeAndPeriod(redis, startAt.Value.Item1, startAt.Value.Item2);
            }

            if (masterDataGenerate)
            {
                var period = MasterDataGenerator.GetPeriod(redis);
                MasterDataGenerator.Import(redis, MasterDataGenerator.Generate(period), false);
            }

            var masterDataPath = Environment.GetEnvironmentVariable("MASTER_DATA_PATH");
            if (masterDataPath != null)
            {
                var json = await System.IO.File.ReadAllBytesAsync(masterDataPath);
                MasterDataGenerator.Import(redis, json, true);
                Api.InitializeGameData(redis, redisConfig);
                return;
            }

            var masterData = new MasterData(redis);

            var cts = new CancellationTokenSource();
            if (apiMode)
            {
                var apiPort = Environment.GetEnvironmentVariable("API_PORT") ?? "8080";
                new Api(redis, redisConfig, masterData, stagingMode, apiPort).Start(cts.Token);
            }

            Console.WriteLine("started.");

            if (calcScoreMode)
            {
                new Thread(() => new CalcScore(redis, redisConfig, masterData).Start(cts.Token)).Start();
            }

            if (!batchMode)
            {
                redis.GetSubscriber().Subscribe("restart", (_, message) => {
                    var msg = (string)message;
                    if (msg == "all" || (apiMode && msg == "api") || (calcScoreMode && msg == "calcScore")) {
                        System.Environment.ExitCode = EXIT_CODE_RESTART;
                        cts.Cancel();
                    }
                });
                try {
                    await Task.Delay(Timeout.Infinite, cts.Token);
                } catch (TaskCanceledException) { /* ignored */ }
            }
        }
    }
}
