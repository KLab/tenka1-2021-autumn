using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text.Json.Serialization;
using StackExchange.Redis;

namespace api
{
    internal class MasterData
    {
        internal class Resource
        {
            [JsonPropertyName("id")]
            public int Id { get; }

            [JsonPropertyName("x")]
            public int X { get; }

            [JsonPropertyName("y")]
            public int Y { get; }

            [JsonPropertyName("t0")]
            public int T0 { get; }

            [JsonPropertyName("t1")]
            public int T1 { get; }

            [JsonPropertyName("type")]
            public string Type { get; }

            [JsonPropertyName("weight")]
            public int Weight { get; }

            public Resource(int id, int x, int y, int t0, int t1, string type, int weight)
            {
                Id = id;
                X = x;
                Y = y;
                T0 = t0;
                T1 = t1;
                Type = type;
                Weight = weight;
            }
        }

        public long GameStartTime { get; }
        public long GamePeriod { get; }
        public ImmutableList<Resource> Resources { get; }

        private readonly ImmutableDictionary<int, Resource> _resource;

        public MasterData(ConnectionMultiplexer redis)
        {
            var db = redis.GetDatabase();

            var startAt = db.StringGet("start_at");
            if (startAt.IsNull)
            {
                throw new Exception("start_at is not set");
            }

            var period = db.StringGet("period");
            if (period.IsNull)
            {
                throw new Exception("period is not set");
            }

            GameStartTime = long.Parse(startAt.ToString());
            GamePeriod = long.Parse(period.ToString());

            var resources = new List<Resource>();
            var resourcesDict = new Dictionary<int, Resource>();
            foreach (var val in db.ListRange("resource"))
            {
                var a = val.ToString().Split(' ');
                Debug.Assert(a.Length == 7);
                var r = new Resource(
                    id: int.Parse(a[0]),
                    x: int.Parse(a[1]),
                    y: int.Parse(a[2]),
                    t0: int.Parse(a[3]),
                    t1: int.Parse(a[4]),
                    type: a[5],
                    weight: int.Parse(a[6])
                );
                resources.Add(r);
                resourcesDict.Add(r.Id, r);
            }

            Resources = resources.ToImmutableList();
            _resource = resourcesDict.ToImmutableDictionary();
        }

        public IReadOnlyDictionary<int, Resource> ResourcesWhere(long t0, long t1)
        {
            return Resources.Where(r => !(t1 <= r.T0 || r.T1 <= t0)).ToDictionary(r => r.Id);
        }

        public Resource? GetResource(int id)
        {
            return _resource.TryGetValue(id, out var r) ? r : null;
        }
    }
}
