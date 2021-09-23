using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace api
{
    internal static class MasterDataGenerator
    {
        public static void SetTime(ConnectionMultiplexer redis, int startOffset)
        {
            var db = redis.GetDatabase();

            db.StringSet("start_at", Api.GetTime() + startOffset);

            var period = db.StringGet("period");
            if (period.IsNull)
            {
                db.StringSet("period", 1000000);
            }
        }

        public static void SetPeriod(ConnectionMultiplexer redis, int period)
        {
            var db = redis.GetDatabase();
            db.StringSet("period", period);
        }

        public static void SetTimeAndPeriod(ConnectionMultiplexer redis, long startAt, int period)
        {
            var db = redis.GetDatabase();
            db.StringSet("start_at", startAt);
            db.StringSet("period", period);
        }

        public static int GetPeriod(ConnectionMultiplexer redis)
        {
            var db = redis.GetDatabase();
            return int.Parse(db.StringGet("period").ToString());
        }

        private static double GetNormRandom(Random random)
        {
            double x = random.NextDouble();
            double y = random.NextDouble();
            double z = Math.Sqrt(-2.0 * Math.Log(x)) * Math.Cos(2.0 * Math.PI * y);

            return z;
        }

        private static double GetNormRandom(Random random, double mu, double sigma)
        {
            return (mu + sigma * GetNormRandom(random));
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class ConfigData
        {
            [JsonPropertyName("seed")]
            public int Seed { get; }

            [JsonPropertyName("resource_time_resolution")]
            public int ResourceTimeResolution { get; }

            [JsonPropertyName("target_num_resource")]
            public int TargetNumResource { get; }

            [JsonPropertyName("min_num_resource")]
            public int MinNumResource { get; }

            [JsonPropertyName("max_num_resource")]
            public int MaxNumResource { get; }

            [JsonPropertyName("weight_end")]
            public int WeightEnd { get; }

            [JsonPropertyName("types")]
            public TypeData[] Types { get; }

            [JsonConstructor]
            public ConfigData(int seed, int resourceTimeResolution, int targetNumResource, int minNumResource, int maxNumResource, int weightEnd, TypeData[] types) =>
                (Seed, ResourceTimeResolution, TargetNumResource, MinNumResource, MaxNumResource, WeightEnd, Types) =
                (seed, resourceTimeResolution, targetNumResource, minNumResource, maxNumResource, weightEnd, types);
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class TypeData
        {
            [JsonPropertyName("type")]
            public string Type { get; }

            [JsonPropertyName("min_time")]
            public int MinTime { get; }

            [JsonPropertyName("max_time")]
            public int MaxTime { get; }

            [JsonPropertyName("probability")]
            public int Probability { get; }

            [JsonPropertyName("weight_params")]
            public WeightParam[] WeightParams { get; }

            [JsonConstructor]
            public TypeData(string type, int minTime, int maxTime, int probability, WeightParam[] weightParams) =>
                (Type, MinTime, MaxTime, Probability, WeightParams) =
                (type, minTime, maxTime, probability, weightParams);
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class WeightParam
        {
            [JsonPropertyName("start")]
            public int Start { get; }

            [JsonPropertyName("mu")]
            public double Mu { get; }

            [JsonPropertyName("sigma")]
            public double Sigma { get; }

            [JsonConstructor]
            public WeightParam(int start, double mu, double sigma) =>
                (Start, Mu, Sigma) =
                (start, mu, sigma);
        }

        private static string GetRandomType(Random random, int sumProbability, IEnumerable<(string, int)> typeProbabilityList)
        {
            var rnd = random.Next(sumProbability);
            foreach (var (type, probability) in typeProbabilityList)
            {
                if (rnd < probability)
                {
                    return type;
                }

                rnd -= probability;
            }

            throw new Exception("invalid type probability");
        }

        private static Dictionary<int, List<(string, int)>> GenerateTypeSchedule(int period, ConfigData configData, Random random)
        {
            Debug.Assert(period > 0 && period % configData.ResourceTimeResolution == 0);
            foreach (var t in configData.Types)
            {
                Debug.Assert(t.MinTime % configData.ResourceTimeResolution == 0);
                Debug.Assert(t.MaxTime % configData.ResourceTimeResolution == 0);
            }

            var typeProbabilityList = configData.Types.Select(t => (t.Type, t.Probability)).ToList();
            var sumProbability = configData.Types.Sum(t => t.Probability);
            var typeTime = configData.Types.ToDictionary(
                t => t.Type,
                t => (t.MinTime / configData.ResourceTimeResolution, t.MaxTime / configData.ResourceTimeResolution));

            var counter = new int[period / configData.ResourceTimeResolution];
            var sumCounter = 0L;
            var typeSchedule = new Dictionary<int, List<(string, int)>>();
            for (var k = 0; k < configData.MinNumResource; k++)
            {
                for (var s = 0; s < counter.Length; )
                {
                    var type = GetRandomType(random, sumProbability, typeProbabilityList);
                    var (min, max) = typeTime[type];
                    var t = random.Next(min, max + 1);
                    if (s + t > counter.Length) s = counter.Length - t;
                    if (!typeSchedule.ContainsKey(s)) typeSchedule.Add(s, new List<(string, int)>());
                    typeSchedule[s].Add((type, t));
                    sumCounter += t;
                    for (var i = 0; i < t; i++) ++ counter[s + i];
                    s += t;
                }
            }

            while (sumCounter < counter.Length * configData.TargetNumResource)
            {
                var type = GetRandomType(random, sumProbability, typeProbabilityList);
                var (min, max) = typeTime[type];
                var t = random.Next(min, max + 1);
                for (var numTry = 0;; numTry++)
                {
                    if (numTry >= 1000000) throw new Exception("GenerateTypeSchedule failure");
                    var s = random.Next(0, counter.Length - t + 1);
                    var ok = true;
                    for (var i = 0; i < t; i++)
                    {
                        if (counter[s + i] >= configData.MaxNumResource)
                        {
                            ok = false;
                            break;
                        }
                    }

                    if (ok)
                    {
                        if (!typeSchedule.ContainsKey(s)) typeSchedule.Add(s, new List<(string, int)>());
                        typeSchedule[s].Add((type, t));
                        sumCounter += t;
                        for (var i = 0; i < t; i++) ++ counter[s + i];
                        break;
                    }
                }
            }

            return typeSchedule;
        }

        private static WeightParam SelectWeightParam(int weightEnd, IReadOnlyList<WeightParam> weightParams, long t0, long period)
        {
            for (var i = 1; i < weightParams.Count; i++)
            {
                var w = weightParams[i];
                // if (t0 / period < w.Start / weightEnd)
                if (t0 * weightEnd < w.Start * period)
                {
                    return weightParams[i-1];
                }
            }

            return weightParams[^1];
        }

        public static byte[] Generate(int period)
        {
            Console.WriteLine("MasterDataGenerator.Generate(period: {0})", period);

            // import config
            var configData = JsonSerializer.Deserialize<ConfigData>(File.ReadAllBytes(@"./map_config.json"))!;
            var typeDict = configData.Types.ToDictionary(t => t.Type, t => t);

            foreach (var t in configData.Types)
            {
                for (var i = 1; i < t.WeightParams.Length; i++)
                {
                    Debug.Assert(t.WeightParams[i - 1].Start < t.WeightParams[i].Start);
                }
            }

            var seed = configData.Seed;
            Console.WriteLine("seed: {0}", seed);
            var random = new Random(seed);

            var typeSchedule = GenerateTypeSchedule(period, configData, random);

            var points = new List<(int, int)>();
            for (var x = 0; x <= 30; x++)
            {
                for (var y = 0; y <= 30; y++)
                {
                    if (x == 0 && y == 0) continue;
                    if (x == 0 && y == 30) continue;
                    if (x == 15 && y == 15) continue;
                    if (x == 30 && y == 0) continue;
                    if (x == 30 && y == 30) continue;
                    points.Add((x, y));
                }
            }

            var resource = new List<object>();
            var endCount = new Dictionary<int, List<(int, int)>>();
            for (var t = 0; t < period / configData.ResourceTimeResolution; t++)
            {
                // 同じ点でリソースが重複しないように管理。リソース消滅時に追加して復活
                if (endCount.ContainsKey(t))
                {
                    foreach (var (x, y) in endCount[t]) points.Add((x, y));
                }

                if (typeSchedule.ContainsKey(t))
                {
                    foreach (var (type, dt) in typeSchedule[t])
                    {
                        var id = resource.Count + 1;

                        // 座標決定
                        var pIndex = random.Next(points.Count);
                        var (x, y) = points[pIndex];
                        points.RemoveAt(pIndex);
                        if (!endCount.ContainsKey(t + dt)) endCount.Add(t + dt, new List<(int, int)>());
                        endCount[t + dt].Add((x, y));

                        // 時間計算
                        var t0 = t * configData.ResourceTimeResolution;
                        var t1 = (t + dt) * configData.ResourceTimeResolution;

                        // 重み決定
                        var weightParam = SelectWeightParam(configData.WeightEnd, typeDict[type].WeightParams, t0, period);
                        var weight = Math.Max(1, (long)Math.Round(GetNormRandom(random, weightParam.Mu, weightParam.Sigma)));

                        resource.Add(new { id, x, y, t0, t1, type, weight });
                    }
                }
            }

            return JsonSerializer.SerializeToUtf8Bytes(new { resource, period });
        }

        public static void Import(ConnectionMultiplexer redis, byte[] json, bool setPeriod)
        {
            Console.WriteLine("Import start");
            var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json);
            var db = redis.GetDatabase();

            if (setPeriod)
            {
                db.StringSet("period", data!["period"].GetInt32());
            }

            db.KeyDelete("resource");

            var tasks = new List<Task>();
            foreach (var r in data!["resource"].EnumerateArray())
            {
                var id = r.GetProperty("id").GetInt32();
                var x = r.GetProperty("x").GetInt32();
                var y = r.GetProperty("y").GetInt32();
                var t0 = r.GetProperty("t0").GetInt32();
                var t1 = r.GetProperty("t1").GetInt32();
                var type = r.GetProperty("type").GetString();
                var weight = r.GetProperty("weight").GetInt32();
                tasks.Add(db.ListRightPushAsync("resource", $"{id} {x} {y} {t0} {t1} {type} {weight}"));
            }

            db.WaitAll(tasks.ToArray());
            Console.WriteLine("Import end");
        }
    }
}
