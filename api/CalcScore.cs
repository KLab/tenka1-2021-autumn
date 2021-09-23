using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace api
{
    internal class CalcScore
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly MasterData _masterData;
        private readonly LoadedLuaScript _getUserDataScript;
        private readonly LoadedLuaScript _adminScript;

        private const int NumRanking = 10;
        private const int CalcDelay = 1000;
        private const int CalcPeriod = 500;
        private const int RankingPeriod = 60000;

        public CalcScore(ConnectionMultiplexer redis, ConfigurationOptions redisConfig, MasterData masterData)
        {
            _redis = redis;
            _masterData = masterData;

            var getUserDataPrepared = LuaScript.Prepare(@"
local user_ids = {}
for s in string.gmatch(@userIds, '([^/]+)') do
  table.insert(user_ids, s)
end
local last_time = tonumber(@lastCalcTime)
local now = tonumber(@now)
local pos_set = {}
for s in string.gmatch(@posSet, '([^ ]+)') do
  table.insert(pos_set, tonumber(s))
end
local pos_list = {}
for i = 1, #pos_set, 2 do
  table.insert(pos_list, string.char(pos_set[i], pos_set[i+1]))
end

local function get_agent_val(user_id, idx, n)
  local v = redis.call('hget', 'agent_'..user_id..'_'..idx, n)
  if v then
    return tonumber(v)
  else
    return false
  end
end

local function calc_cost(dx, dy)
  return math.max(1, math.ceil((dx*dx + dy*dy)^0.5 * 100))
end

local function calc(user_id)
  local data = {}
  for _, v in ipairs(pos_list) do
    data[v] = {}
  end

  for idx = 1, 5 do
    local t0 = get_agent_val(user_id, idx, 't0')
    if t0 then
      local x0 = get_agent_val(user_id, idx, 'x0')
      local y0 = get_agent_val(user_id, idx, 'y0')
      local x1 = get_agent_val(user_id, idx, 'x1')
      local y1 = get_agent_val(user_id, idx, 'y1')
      local t1 = get_agent_val(user_id, idx, 't1')
      local t2 = get_agent_val(user_id, idx, 't2')
      local x3 = get_agent_val(user_id, idx, 'x3')
      local y3 = get_agent_val(user_id, idx, 'y3')
      local a = {}
      local history_key = 'history_'..user_id..'_'..idx
      local len_history = redis.call('llen', history_key)
      for i = 0, len_history-1, 4 do
        local h = redis.call('lrange', history_key, i, i + 3)
        local tt1 = tonumber(h[4])
        if tt1 <= last_time then break end
        local x = tonumber(h[1])
        local y = tonumber(h[2])
        local tt0 = tonumber(h[3])
        table.insert(a, {x, y, tt0, tt1})
      end
      if t1 then
        if t2 then
          if t2 < t1 then
            local xx = (x0*(t1-t2)+x1*(t2-t0))/(t1-t0)
            local yy = (y0*(t1-t2)+y1*(t2-t0))/(t1-t0)
            local t3 = t2 + calc_cost(xx-x3, yy-y3)
            table.insert(a, {x3, y3, t3, now})
          else
            if t2 ~= t1 then
              table.insert(a, {x1, y1, t1, t2})
            end
            local t3 = t2 + calc_cost(x1-x3, y1-y3)
            table.insert(a, {x3, y3, t3, now})
          end
        else
          table.insert(a, {x1, y1, t1, now})
        end
      else
        if t2 then
          table.insert(a, {x0, y0, t0, t2})
          local t3 = t2 + calc_cost(x0-x3, y0-y3)
          table.insert(a, {x3, y3, t3, now})
        else
          table.insert(a, {x0, y0, t0, now})
        end
      end
      for i, v in ipairs(a) do
        local tt0 = v[3]
        local tt1 = v[4]
        if tt0 < tt1 and not (tt1 <= last_time or now <= tt0) then
          if tt0 < last_time then tt0 = last_time end
          if tt1 > now then tt1 = now end
          local pos = string.char(v[1], v[2])
          if data[pos] then
            data[pos][tt0] = (data[pos][tt0] or 0) + 1
            data[pos][tt1] = (data[pos][tt1] or 0) - 1
          end
        end
      end
    end
  end

  local res = {}
  for pos, a in pairs(data) do
    local b = {}
    for t, n in pairs(a) do
      if n > 0 then
        table.insert(b, t)
        table.insert(b, n)
      end
    end
    if #b > 0 then
      local x = string.byte(pos, 1)
      local y = string.byte(pos, 2)
      table.insert(res, x)
      table.insert(res, y)
      table.insert(res, b)
    end
  end
  return res
end

local resp = {}
for _, v in ipairs(user_ids) do
  table.insert(resp, calc(v))
end
return resp
");
            var adminPrepared = LuaScript.Prepare(@"
local user_ids = {}
for s in string.gmatch(@topUserIds, '([^/]+)') do
  table.insert(user_ids, s)
end
local user_idx = {}
for s in string.gmatch(@topUserIdx, '([^/]+)') do
  table.insert(user_idx, tonumber(s))
end
local now = tonumber(@now)

local function convert(a, t0, t1)
  local xx = {}
  local yy = {}
  local tt = {}
  for i = 2, #a, 3 do
    table.insert(xx, a[i])
    table.insert(yy, a[i+1])
    table.insert(tt, tonumber(a[i+2]))
  end
  local r = {}
  for i = 1, #xx do
    if tt[i] >= t1 then break end
    if tt[i] < t0 then
      if i+1 <= #xx then
        if tt[i+1] > t0 then
          local xi = tonumber(xx[i])
          local yi = tonumber(yy[i])
          local ti = tt[i]
          local xj = tonumber(xx[i+1])
          local yj = tonumber(yy[i+1])
          local tj = tt[i+1]
          table.insert(r, (xi*(tj-t0)+xj*(t0-ti))/(tj-ti))
          table.insert(r, (yi*(tj-t0)+yj*(t0-ti))/(tj-ti))
          table.insert(r, t0)
        end
      else
        table.insert(r, xx[i])
        table.insert(r, yy[i])
        table.insert(r, t0)
      end
    else
      table.insert(r, xx[i])
      table.insert(r, yy[i])
      table.insert(r, tt[i])
    end
  end
  return r
end

for i = 1, #user_idx do
  for idx = 1, 5 do
    local r = {}

    local k = 'record_'..user_ids[i]..'_'..idx
    local len_history = redis.call('llen', k)
    if len_history == 0 then
      local start_pos = ({{0, 0}, {0, 30}, {15, 15}, {30, 0}, {30, 30}})[idx]
      r = {tostring(start_pos[1]), tostring(start_pos[2]), tostring(now)}
    else
      local t1 = 1e+100
      local converted = {}
      for j = 0, len_history-1 do
        local h = redis.call('lindex', k, j)
        local a = {}
        for s in string.gmatch(h, '([^ ]+)') do
          table.insert(a, s)
        end
        local t = tonumber(a[1])
        table.insert(converted, convert(a, t, t1))
        if t <= now then break end
        t1 = t
      end
      for j = #converted, 1, -1 do
        for _, v in ipairs(converted[j]) do
          table.insert(r, tostring(v))
        end
      end
    end

    local pub_str = 'M'..tostring(user_idx[i]*5+idx)..' '..tostring(now)..' '..table.concat(r, ' ')
    redis.call('publish', 'admin', pub_str)
  end
end
redis.call('publish', 'admin', 'U')
");
            Debug.Assert(redisConfig.EndPoints.Count == 1);
            var server = redis.GetServer(redisConfig.EndPoints[0]);
            _getUserDataScript = getUserDataPrepared.Load(server);
            _adminScript = adminPrepared.Load(server);
        }

        public void Start(CancellationToken cts)
        {
            var db = _redis.GetDatabase();
            var userIdList = new List<string>();
            var userDataDict = new Dictionary<string, UserData>();
            var userIdxDict = new Dictionary<string, int>();
            var topUserIds = new List<string>();
            var topUserIdx = new List<int>();
            var sw = new Stopwatch();

            var lastCalcTimeVal = db.StringGet("last_calc_time");
            var lastCalcTime = lastCalcTimeVal.IsNull ? 0 : long.Parse(lastCalcTimeVal);
            var initFlag = true;
            while (!cts.IsCancellationRequested)
            {
                while (true)
                {
                    var t = Api.GetTime() - _masterData.GameStartTime - CalcDelay;
                    if (t >= lastCalcTime + CalcPeriod) break;
                    Thread.Sleep((int)(lastCalcTime + CalcPeriod - t));
                }

                sw.Restart();

                var now = lastCalcTime + CalcPeriod;
                Console.WriteLine($"calc start {now} {Api.GetTime() - _masterData.GameStartTime}");
                if (now <= _masterData.GamePeriod)
                {
                    var resources = _masterData.ResourcesWhere(lastCalcTime, now);
                    var posSet = string.Join(' ', resources.Values.Select(r => $"{r.X} {r.Y}").ToHashSet());

                    foreach (var userIdValue in db.HashValues("user_token"))
                    {
                        var userId = (string) userIdValue;
                        if (userDataDict.ContainsKey(userId)) continue;
                        userIdList.Add(userId);
                        userDataDict.Add(userId, new UserData());
                        userIdxDict.Add(userId, userIdList.Count);
                    }

                    if (initFlag)
                    {
                        initFlag = false;
                        if (lastCalcTime > 0)
                        {
                            var d = db.HashGet("user_data_backup", lastCalcTime);
                            foreach (var x in JsonDocument.Parse((byte[]) d).RootElement.EnumerateObject())
                            {
                                userDataDict[x.Name].Import(x.Value);
                            }
                        }
                    }

                    db.StringSet("calc_time", now);

                    const int numCalc = 50;
                    var sumDict = new Dictionary<int, int[]>();
                    for (var i = 0; i < userIdList.Count; i += numCalc)
                    {
                        var a = new List<string>();
                        for (var j = i; j < userIdList.Count && j < i + numCalc; j++)
                        {
                            a.Add(userIdList[j]);
                        }

                        var userIds = string.Join('/', a);

                        var resUserData = (RedisResult[]) db.Wait(_getUserDataScript.EvaluateAsync(db, new
                        {
                            userIds,
                            lastCalcTime,
                            now,
                            posSet,
                        }));
                        Debug.Assert(a.Count == resUserData.Length);

                        for (var j = 0; j < a.Count; j++)
                        {
                            var userId = a[j];
                            var res = (RedisResult[])resUserData[j];
                            userDataDict[userId].Update(res, lastCalcTime, now, sumDict, resources, _masterData);
                        }
                    }

                    var ranking = new List<(double, string)>();
                    var scoreHashEntries = new List<HashEntry>();
                    var amountHashEntries = new List<HashEntry>();
                    var scoreDict = new Dictionary<string, (string, double)[]>();
                    var userDataExported = new Dictionary<string, object>();
                    foreach (var (userId, x) in userDataDict)
                    {
                        var (s, d) = x.Calc(sumDict, resources);
                        ranking.Add((s, userId));
                        scoreDict[userId] = d;
                        scoreHashEntries.AddRange(d.Select(kv => new HashEntry($"{userId}_{kv.Item1}", kv.Item2)));
                        amountHashEntries.AddRange(
                            x.ScoreProgress.Where(kv => kv.Value > 0)
                                .Select(kv => new HashEntry($"{userId}_{kv.Key}", kv.Value))
                        );
                        var y = x.Export();
                        if (y != null) userDataExported.Add(userId, y);
                    }

                    var backupJson = JsonSerializer.SerializeToUtf8Bytes(userDataExported);

                    var scoreHashTask = db.HashSetAsync("score_hash", scoreHashEntries.ToArray());
                    var amountHashTask = db.HashSetAsync("amount_hash", amountHashEntries.ToArray());
                    var backupTask = db.HashSetAsync("user_data_backup", now, backupJson);
                    db.WaitAll(scoreHashTask, amountHashTask, backupTask);

                    db.StringSet("last_calc_time", now);
                    lastCalcTime = now;

                    ranking.Sort((l, r) =>
                    {
                        var (ls, ln) = l;
                        var (rs, rn) = r;
                        var x = ls.CompareTo(rs);
                        if (x != 0) return -x;
                        return string.Compare(ln, rn, StringComparison.Ordinal);
                    });

                    var ranks = new List<int> { 1 };
                    for (var i = 1; i < ranking.Count; i++)
                    {
                        // ReSharper disable once CompareOfFloatsByEqualityOperator
                        ranks.Add(ranking[i-1].Item1 == ranking[i].Item1 ? ranks[i-1]: i+1);
                    }

                    var rankingData = new List<object>();
                    topUserIds = new List<string>();
                    topUserIdx = new List<int>();
                    for (var i = 0; i < ranking.Count && i < NumRanking; i++)
                    {
                        var (point, userId) = ranking[i];
                        var rank = ranks[i];
                        rankingData.Add(new {point, userId, rank});
                        topUserIds.Add(userId);
                        topUserIdx.Add(userIdxDict[userId]);
                    }

                    var publishTasks = new List<Task>
                    {
                        db.PublishAsync("admin",
                            "R" + JsonSerializer.Serialize(new
                            {
                                type = "ranking", ranking = rankingData, owned_resource = Array.Empty<int>(),
                            }))
                    };

                    for (var i = 0; i < ranking.Count; i++)
                    {
                        var (point, userId) = ranking[i];
                        if (i >= NumRanking)
                        {
                            var rank = ranks[i];
                            rankingData[NumRanking - 1] = new {point, userId, rank};
                        }

                        publishTasks.Add(db.PublishAsync(userId, "R" + JsonSerializer.Serialize(new
                        {
                            type = "ranking",
                            ranking = rankingData,
                            owned_resource = scoreDict[userId].Select(kv => new
                            {
                                type = kv.Item1,
                                amount = kv.Item2,
                            }).ToArray(),
                        })));
                    }

                    db.WaitAll(publishTasks.ToArray());

                    if (now % RankingPeriod == 0)
                    {
                        var rankingDataForRedis = ranking.Select((x, _) => new SortedSetEntry(x.Item2, x.Item1)).ToArray();
                        var task1 = db.SortedSetAddAsync($"ranking_{now}", rankingDataForRedis);
                        var task3 = db.ListRightPushAsync("ranking_times", now);
                        db.WaitAll(task1, task3);
                        Console.WriteLine($"Ranking update {now}");
                    }
                }

                var nowAdmin = Api.GetTime() - _masterData.GameStartTime - Api.AdminDelay;
                if (topUserIds.Count > 0 && nowAdmin >= 0)
                {
                    _adminScript.Evaluate(db, new
                    {
                        topUserIds = string.Join('/', topUserIds),
                        topUserIdx = string.Join('/', topUserIdx),
                        now = nowAdmin,
                    });
                }

                Console.WriteLine($"calc end   {sw.ElapsedTicks*1e-6}");

                if (lastCalcTime >= _masterData.GamePeriod && nowAdmin >= _masterData.GamePeriod) break;
            }
        }

        private class UserData
        {
            private long _lastCalcTime;
            private readonly Dictionary<int, int[]> _sum;
            private readonly Dictionary<int, double> _scoreProgress;
            private readonly Dictionary<string, double> _scoreFixed;

            public IReadOnlyDictionary<int, double> ScoreProgress => _scoreProgress;

            public UserData()
            {
                _lastCalcTime = 0;
                _sum = new Dictionary<int, int[]>();
                _scoreProgress = new Dictionary<int, double>();
                _scoreFixed = new Dictionary<string, double>
                {
                    ["A"] = 0.0,
                    ["B"] = 0.0,
                    ["C"] = 0.0,
                };
            }

            public void Update(RedisResult[] res, long t0, long t1, Dictionary<int, int[]> sumDict, IReadOnlyDictionary<int, MasterData.Resource> resources, MasterData masterData)
            {
                Debug.Assert(_lastCalcTime != t1);
                _lastCalcTime = t1;
                _sum.Clear();

                var scoresRemoved = new Dictionary<string, List<double>>
                {
                    ["A"] = new(),
                    ["B"] = new(),
                    ["C"] = new(),
                };

                foreach (var id in _scoreProgress.Keys.Where(id => !resources.ContainsKey(id)).ToImmutableList())
                {
                    var r = masterData.GetResource(id)!;
                    var score = _scoreProgress[id] * r.Weight;
                    scoresRemoved[r.Type].Add(score);
                    _scoreProgress.Remove(id);
                }

                foreach (var (type, v) in scoresRemoved)
                {
                    if (v.Count > 0)
                    {
                        _scoreFixed[type] += Sum(v);
                    }
                }

                var a = new Dictionary<(int, int), SortedDictionary<long, int>>();
                for (var i = 0; i < res.Length; i += 3)
                {
                    var x = (int)res[i];
                    var y = (int)res[i+1];
                    var b = (RedisResult[])res[i+2];
                    var c = new SortedDictionary<long, int>();
                    for (var j = 0; j < b.Length; j += 2) c.Add((int)b[j], (int)b[j+1]);
                    a.Add((x, y), c);
                }

                foreach (var (id, r) in resources)
                {
                    if (!a.TryGetValue((r.X, r.Y), out var b)) continue;
                    var tt0 = Math.Max(t0, r.T0);
                    var tt1 = Math.Min(t1, r.T1);
                    var s = 0;
                    foreach (var (t, v) in b)
                    {
                        if (t >= tt0) break;
                        s += v;
                    }

                    var c = new int[tt1 - tt0];
                    for (var i = 0; i < c.Length; i++)
                    {
                        var t = tt0 + i;
                        if (b.ContainsKey(t)) s += b[t];
                        c[i] = s;
                    }

                    _sum.Add(id, c);
                    if (!sumDict.ContainsKey(id))
                    {
                        sumDict.Add(id, new int[c.Length]);
                    }

                    for (var i = 0; i < c.Length; i++)
                    {
                        sumDict[id][i] += c[i];
                    }
                }
            }

            public (double, (string, double)[]) Calc(IReadOnlyDictionary<int, int[]> sumDict, IReadOnlyDictionary<int, MasterData.Resource> resources)
            {
                foreach (var (id, a) in _sum)
                {
                    var totalSum = sumDict[id];
                    Debug.Assert(a.Length == totalSum.Length);
                    if (!_scoreProgress.ContainsKey(id)) _scoreProgress[id] = 0;
                    for (var i = 0; i < a.Length; i++)
                    {
                        if (a[i] > 0)
                        {
                            _scoreProgress[id] += (double) a[i] / totalSum[i];
                        }
                    }
                }

                var scores = new Dictionary<string, List<double>>
                {
                    ["A"] = new(),
                    ["B"] = new(),
                    ["C"] = new(),
                };

                foreach (var (id, v) in _scoreProgress)
                {
                    var r = resources[id];
                    scores[r.Type].Add(v * r.Weight);
                }

                var s = new[]
                {
                    _scoreFixed["A"] + Sum(scores["A"]),
                    _scoreFixed["B"] + Sum(scores["B"]),
                    _scoreFixed["C"] + Sum(scores["C"]),
                };
                var sd = new[]
                {
                    ("A", s[0] / 1000),
                    ("B", s[1] / 1000),
                    ("C", s[2] / 1000),
                };
                Array.Sort(s);
                return ((100 * s[0] + 10 * s[1] + s[2]) * 1e-5, sd);
            }

            public object? Export()
            {
                var a = new List<object>();
                foreach (var (k, v) in _scoreProgress)
                {
                    if (v == 0) continue;
                    a.Add(k);
                    a.Add(v);
                }

                var b = new Dictionary<string, double>();
                foreach (var (k, v) in _scoreFixed)
                {
                    if (v == 0) continue;
                    b[k] = v;
                }

                var res = new Dictionary<string, object>();
                if (a.Count > 0) res["a"] = a;
                if (b.Count > 0) res["b"] = b;
                return res.Count != 0 ? res : null;
            }

            public void Import(JsonElement data)
            {
                _scoreProgress.Clear();
                foreach (var x in data.EnumerateObject())
                {
                    switch (x.Name)
                    {
                        case "a":
                            int? k = null;
                            foreach (var y in x.Value.EnumerateArray())
                            {
                                if (k.HasValue)
                                {
                                    _scoreProgress.Add(k.Value, y.GetDouble());
                                    k = null;
                                }
                                else
                                {
                                    k = y.GetInt32();
                                }
                            }

                            break;
                        case "b":
                            foreach (var y in x.Value.EnumerateObject())
                            {
                                _scoreFixed[y.Name] = y.Value.GetDouble();
                            }

                            break;
                        default:
                            throw new Exception("invalid value");
                    }
                }
            }

            private static double Sum(List<double> a)
            {
                if (a.Count == 0) return 0;

                while (a.Count > 1)
                {
                    a.Sort();
                    a[0] += a[1];
                    a.RemoveAt(1);
                }

                return a[0];
            }
        }
    }
}
