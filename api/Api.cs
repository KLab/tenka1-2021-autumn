using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using StackExchange.Redis;

namespace api
{
    internal class Api
    {
        private const string ProdHealthEndpoint = "/api/health";
        private const string ProdGameEndpoint = "/api/game/";
        private const string ProdMoveEndpoint = "/api/move/";
        private const string ProdWillMoveEndpoint = "/api/will_move/";
        private const string ProdResourcesEndpoint = "/api/resources/";
        private const string ProdEventEndpoint = "/event/";
        private const string StgHealthEndpoint = "/staging/api/health";
        private const string StgGameEndpoint = "/staging/api/game/";
        private const string StgMoveEndpoint = "/staging/api/move/";
        private const string StgWillMoveEndpoint = "/staging/api/will_move/";
        private const string StgResourcesEndpoint = "/staging/api/resources/";
        private const string StgEventEndpoint = "/staging/event/";
        private const string AdminEventEndpoint = "/event/admin/39lnrqo51f5";

        private string HealthEndpoint => _stagingMode ? StgHealthEndpoint : ProdHealthEndpoint;
        private string GameEndpoint => _stagingMode ? StgGameEndpoint : ProdGameEndpoint;
        private string MoveEndpoint => _stagingMode ? StgMoveEndpoint : ProdMoveEndpoint;
        private string WillMoveEndpoint => _stagingMode ? StgWillMoveEndpoint : ProdWillMoveEndpoint;
        private string ResourcesEndpoint => _stagingMode ? StgResourcesEndpoint : ProdResourcesEndpoint;
        private string EventEndpoint => _stagingMode ? StgEventEndpoint : ProdEventEndpoint;

        private static readonly byte[] SseHeader = Encoding.UTF8.GetBytes("data: ");
        private static readonly byte[] SseFooter = Encoding.UTF8.GetBytes("\n\n");
        private static readonly byte[] SsePing = Encoding.UTF8.GetBytes("event: ping\ndata: \n\n");

        private static readonly Regex ReNum = new(@"^([0-9]|[1-9][0-9]{1,8})$", RegexOptions.Compiled);
        private static readonly Regex ReToken = new(@"^[0-9a-z]+$", RegexOptions.Compiled);

        private readonly ConnectionMultiplexer _redis;
        private readonly MasterData _masterData;
        private readonly bool _stagingMode;
        private readonly LoadedLuaScript _runGameScript;
        private readonly LoadedLuaScript _runMoveScript;
        private readonly LoadedLuaScript _runTimeLimitScript;
        private readonly HttpListener _listener;

        private const int NumAgent = 5;
        private const int AreaSize = 30;
        private const int MaxNumGetResources = 20;

        private const int SseCancelTime = 5000;
        private const int ResourceTime = 10000;
        private const int SseTimeLimit = 1000;
        private const int GameTimeLimit = 1000;
        private const int MoveTimeLimit = 100;
        private const int ResourcesTimeLimit = 1000;
        public const int AdminDelay = 2000;

        public Api(ConnectionMultiplexer redis, ConfigurationOptions redisConfig, MasterData masterData, bool stagingMode, string port)
        {
            _redis = redis;
            _masterData = masterData;
            _stagingMode = stagingMode;

            var gamePrepared = LuaScript.Prepare(@"
local user_id = @userId
local now = tonumber(@now)

local function get_agent_val(idx, n)
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

local agent = {}
for idx = 1, 5 do

local x0 = get_agent_val(idx, 'x0')
local y0 = get_agent_val(idx, 'y0')
local t0 = get_agent_val(idx, 't0')
local x1 = get_agent_val(idx, 'x1')
local y1 = get_agent_val(idx, 'y1')
local t1 = get_agent_val(idx, 't1')
local t2 = get_agent_val(idx, 't2')
local x3 = get_agent_val(idx, 'x3')
local y3 = get_agent_val(idx, 'y3')

if not t0 then
  local start_pos = ({{0, 0}, {0, 30}, {15, 15}, {30, 0}, {30, 30}})[tonumber(idx)]
  x0 = start_pos[1]
  y0 = start_pos[2]
  t0 = now
end

local res = {}
if t1 then
  if t2 then
    if t2 < t1 then
      local xx = (x0*(t1-t2)+x1*(t2-t0))/(t1-t0)
      local yy = (y0*(t1-t2)+y1*(t2-t0))/(t1-t0)
      local t3 = t2 + calc_cost(xx-x3, yy-y3)
      res = {x0, y0, t0, xx, yy, t2, x3, y3, t3}
    else
      local t3 = t2 + calc_cost(x1-x3, y1-y3)
      if t2 == t1 then
        res = {x0, y0, t0, x1, y1, t1, x3, y3, t3}
      else
        res = {x0, y0, t0, x1, y1, t1, x1, y1, t2, x3, y3, t3}
      end
    end
  else
    res = {x0, y0, t0, x1, y1, t1}
  end
else
  if t2 then
    local t3 = t2 + calc_cost(x0-x3, y0-y3)
    res = {x0, y0, t0, x0, y0, t2, x3, y3, t3}
  else
    res = {x0, y0, t0}
  end
end

local r = {}
for _, v in ipairs(res) do
  table.insert(r, tostring(v))
end
table.insert(agent, r)

end

local score = {}
local score_a = redis.call('hget', 'score_hash', user_id..'_A')
local score_b = redis.call('hget', 'score_hash', user_id..'_B')
local score_c = redis.call('hget', 'score_hash', user_id..'_C')
if score_a and score_b and score_c then
  score = {score_a, score_b, score_c}
end

return {agent, score}
");
            var movePrepared = LuaScript.Prepare(@"
local user_id = @userId
local idx = @idx
local x = tonumber(@x)
local y = tonumber(@y)
local now = tonumber(@now)
local time = tonumber(@time)

local calc_time = redis.call('get', 'calc_time')
if calc_time and now <= tonumber(calc_time) then
  return {}
end

local function get_agent_val(n)
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

local function push_history(xx, yy, t1, t2)
  if t1 < t2 then
    redis.call('lpush', 'history_'..user_id..'_'..idx, t2, t1, yy, xx)
  end
end

local x0 = get_agent_val('x0')
local y0 = get_agent_val('y0')
local t0 = get_agent_val('t0')
local x1 = get_agent_val('x1')
local y1 = get_agent_val('y1')
local t1 = get_agent_val('t1')
local t2 = get_agent_val('t2')
local x3 = get_agent_val('x3')
local y3 = get_agent_val('y3')

if not t0 then
  local start_pos = ({{0, 0}, {0, 30}, {15, 15}, {30, 0}, {30, 30}})[tonumber(idx)]
  x0 = start_pos[1]
  y0 = start_pos[2]
  t0 = now
end

if t2 and t2 <= now then
  if t1 then
    if t2 < t1 then
      x0 = (x0*(t1-t2)+x1*(t2-t0))/(t1-t0)
      y0 = (y0*(t1-t2)+y1*(t2-t0))/(t1-t0)
    else
      push_history(x1, y1, t1, t2)
      x0 = x1
      y0 = y1
    end
  end
  t0 = t2
  x1 = x3
  y1 = y3
  t1 = t0 + calc_cost(x0-x1, y0-y1)
end
t2 = false
x3 = false
y3 = false

if t1 and t1 <= now then
  push_history(x1, y1, t1, now)
  x0 = x1
  y0 = y1
  t0 = now
  x1 = false
  y1 = false
  t1 = false
end

if now == time then
  if t1 then
    if t1 > now then
      x0 = (x0*(t1-now)+x1*(now-t0))/(t1-t0)
      y0 = (y0*(t1-now)+y1*(now-t0))/(t1-t0)
    else
      push_history(x0, y0, t1, now)
    end
  else
    push_history(x0, y0, t0, now)
  end
  t0 = now
  x1 = x
  y1 = y
  t1 = t0 + calc_cost(x0-x1, y0-y1)
else
  t2 = time
  x3 = x
  y3 = y
end

if t1 and t0 == t1 then
  x0 = x1
  y0 = y1
  x1 = false
  y1 = false
  t1 = false
end

redis.call('hset', 'agent_'..user_id..'_'..idx, 'x0', x0, 'y0', y0, 't0', t0)
if t1 then
  redis.call('hset', 'agent_'..user_id..'_'..idx, 'x1', x1, 'y1', y1, 't1', t1)
else
  redis.call('hdel', 'agent_'..user_id..'_'..idx, 'x1', 'y1', 't1')
end
if t2 then
  redis.call('hset', 'agent_'..user_id..'_'..idx, 't2', t2, 'x3', x3, 'y3', y3)
else
  redis.call('hdel', 'agent_'..user_id..'_'..idx, 't2', 'x3', 'y3')
end

local res = {}
if t1 then
  if t2 then
    if t2 < t1 then
      local xx = (x0*(t1-t2)+x1*(t2-t0))/(t1-t0)
      local yy = (y0*(t1-t2)+y1*(t2-t0))/(t1-t0)
      local t3 = t2 + calc_cost(xx-x3, yy-y3)
      res = {x0, y0, t0, xx, yy, t2, x3, y3, t3}
    else
      local t3 = t2 + calc_cost(x1-x3, y1-y3)
      if t2 == t1 then
        res = {x0, y0, t0, x1, y1, t1, x3, y3, t3}
      else
        res = {x0, y0, t0, x1, y1, t1, x1, y1, t2, x3, y3, t3}
      end
    end
  else
    res = {x0, y0, t0, x1, y1, t1}
  end
else
  if t2 then
    local t3 = t2 + calc_cost(x0-x3, y0-y3)
    res = {x0, y0, t0, x0, y0, t2, x3, y3, t3}
  else
    res = {x0, y0, t0}
  end
end

local r = {}
for _, v in ipairs(res) do
  table.insert(r, tostring(v))
end
local move_str = tostring(now)..' '..table.concat(r, ' ')
redis.call('publish', user_id, 'M'..idx..' '..move_str)
redis.call('lpush', 'record_'..user_id..'_'..idx, move_str)
return r
");
            var timeLimitPrepared = LuaScript.Prepare(@"
local field = @field
local now = tonumber(@now)
local time_limit = tonumber(@timeLimit)
local t = redis.call('hget', 'unlock_time', field)
if t and now < tonumber(t) then
  return tostring(t)
end
redis.call('hset', 'unlock_time', field, now + time_limit)
return 'ok'
");
            Debug.Assert(redisConfig.EndPoints.Count == 1);
            var server = redis.GetServer(redisConfig.EndPoints[0]);
            _runGameScript = gamePrepared.Load(server);
            _runMoveScript = movePrepared.Load(server);
            _runTimeLimitScript = timeLimitPrepared.Load(server);

            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://*:{port}/");
        }

        public void Start(CancellationToken cts)
        {
            _listener.Start();
            _listener.BeginGetContext(OnRequest, _listener);
            cts.Register(() => _listener.Close());
        }

        public static void InitializeGameData(ConnectionMultiplexer redis, ConfigurationOptions redisConfig)
        {
            Debug.Assert(redisConfig.EndPoints.Count == 1);
            var server = redis.GetServer(redisConfig.EndPoints[0]);
            var db = redis.GetDatabase();

            db.KeyDelete(server.Keys(pattern: "ranking_*").ToArray());
            db.KeyDelete("user_data_backup");
            db.KeyDelete("ranking_times");
            db.KeyDelete("unlock_time");
            db.KeyDelete("score_hash");
            db.KeyDelete("amount_hash");
            db.KeyDelete("calc_time");
            db.KeyDelete("last_calc_time");
            foreach (var x in db.HashValues("user_token"))
            {
                var userId = x.ToString();
                for (var idx = 1; idx <= NumAgent; idx++)
                {
                    db.KeyDelete($"agent_{userId}_{idx}");
                    db.KeyDelete($"history_{userId}_{idx}");
                    db.KeyDelete($"record_{userId}_{idx}");
                }
            }
        }

        private void OnRequest(IAsyncResult result)
        {
            if (!_listener.IsListening) return;

            var context = _listener.EndGetContext(result);
            _listener.BeginGetContext(OnRequest, _listener);

            new Thread(() => OnRequest(context)).Start();
        }

        private void OnRequest(HttpListenerContext context)
        {
            var rawUrl = context.Request.RawUrl;
            Debug.Assert(rawUrl != null, nameof(rawUrl) + " != null");
            // Console.WriteLine("rawUrl = " + rawUrl);
            context.Response.AppendHeader("Access-Control-Allow-Origin", "*");
            if (rawUrl == AdminEventEndpoint)
            {
                try
                {
                    RunAdminSse(context.Response);
                }
                catch (HttpListenerException)
                {
                    context.Response.Abort();
                }
                catch (Exception e2)
                {
                    Console.Error.WriteLine(e2);
                    context.Response.Abort();
                }
            }
            else if (rawUrl.StartsWith(EventEndpoint, StringComparison.Ordinal))
            {
                try
                {
                    RunSse(context.Response, rawUrl);
                }
                catch (HttpListenerException)
                {
                    context.Response.Abort();
                }
                catch (Exception e2)
                {
                    Console.Error.WriteLine(e2);
                    context.Response.Abort();
                }
            }
            else if (rawUrl.StartsWith(GameEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunGame);
            }
            else if (rawUrl.StartsWith(MoveEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunMove);
            }
            else if (rawUrl.StartsWith(WillMoveEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunWillMove);
            }
            else if (rawUrl.StartsWith(ResourcesEndpoint, StringComparison.Ordinal))
            {
                OnRequestMain(context, rawUrl, RunResources);
            }
            else if (rawUrl.StartsWith(HealthEndpoint, StringComparison.Ordinal))
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.OK;
                response.Close();
            }
            else
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
            }
        }

        private static void OnRequestMain(HttpListenerContext context, string rawUrl, Func<string, byte[]?> func)
        {
            byte[]? responseString;
            try
            {
                responseString = func(rawUrl);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                context.Response.ContentType = "text/plain";
                try
                {
                    context.Response.Close();
                }
                catch (HttpListenerException)
                {
                    context.Response.Abort();
                }
                catch (Exception e2)
                {
                    Console.Error.WriteLine(e2);
                    context.Response.Abort();
                }

                return;
            }

            if (responseString == null)
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
                return;
            }

            try
            {
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.OK;
                response.OutputStream.Write(responseString);
                response.Close();
            }
            catch (HttpListenerException)
            {
                context.Response.Abort();
            }
            catch (Exception e2)
            {
                Console.Error.WriteLine(e2);
                context.Response.Abort();
            }
        }

        private long? WaitUnlock(string type, string userId, int timeLimit)
        {
            var now = GetTime() - _masterData.GameStartTime;
            if (now < 0) return now;

            var unlockTime = -1;
            for (;;)
            {
                var ut = GetSetTimeLimit(type, userId, now, timeLimit);
                if (ut < 0)
                {
                    break;
                }

                if (unlockTime < 0)
                {
                    unlockTime = ut;
                }
                else if (unlockTime != ut)
                {
                    return null;
                }

                Thread.Sleep((int)Math.Max(1, unlockTime - now));
                now = GetTime() - _masterData.GameStartTime;
                while (now < unlockTime)
                {
                    Thread.Sleep(1);
                    now = GetTime() - _masterData.GameStartTime;
                }
            }

            return now;
        }

        private byte[]? RunGame(string rawUrl)
        {
            // /api/game/([0-9a-z]+)
            Debug.Assert(rawUrl.StartsWith(GameEndpoint, StringComparison.Ordinal));
            var token = rawUrl.Substring(GameEndpoint.Length);

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock("game", userId, GameTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= _masterData.GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            var db = _redis.GetDatabase();
            var res = (RedisResult[])_runGameScript.Evaluate(db, new { userId, now });
            Debug.Assert(res.Length == 2);

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["now"] = now,
                ["agent"] = ConvertAgentData((RedisResult[]) res[0]),
                ["resource"] = GetResourceData(now),
                ["next_resource"] = GetNextResource(now),
                ["owned_resource"] = GetOwnedResource((RedisResult[]) res[1]),
            });
        }

        private byte[]? RunMove(string rawUrl)
        {
            // /api/move/([0-9a-z]+)/([0-9]+)-([0-9]+)-([0-9]+)
            Debug.Assert(rawUrl.StartsWith(MoveEndpoint, StringComparison.Ordinal));
            var paramStr = rawUrl.Substring(MoveEndpoint.Length);

            var param = paramStr.Split('/');
            if (param.Length != 2) return null;

            var param1 = param[1].Split("-");
            if (param1.Length != 3) return null;
            if (!(ReToken.IsMatch(param[0]) && ReNum.IsMatch(param1[0]) && ReNum.IsMatch(param1[1]) && ReNum.IsMatch(param1[2]))) return null;

            var idx = int.Parse(param1[0]);
            var x = int.Parse(param1[1]);
            var y = int.Parse(param1[2]);
            return RunMoveMain(param[0], idx, x, y, null);
        }

        private byte[]? RunWillMove(string rawUrl)
        {
            // /api/will_move/([0-9a-z]+)/([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)
            Debug.Assert(rawUrl.StartsWith(WillMoveEndpoint, StringComparison.Ordinal));
            var paramStr = rawUrl.Substring(WillMoveEndpoint.Length);

            var param = paramStr.Split('/');
            if (param.Length != 2) return null;

            var param1 = param[1].Split("-");
            if (param1.Length != 4) return null;
            if (!(ReToken.IsMatch(param[0]) && ReNum.IsMatch(param1[0]) && ReNum.IsMatch(param1[1]) && ReNum.IsMatch(param1[2]) && ReNum.IsMatch(param1[3]))) return null;

            var idx = int.Parse(param1[0]);
            var x = int.Parse(param1[1]);
            var y = int.Parse(param1[2]);
            var t = int.Parse(param1[3]);
            return RunMoveMain(param[0], idx, x, y, t);
        }

        private byte[]? RunMoveMain(string token, int idx, int x, int y, int? t)
        {
            if (!(1 <= idx && idx <= NumAgent && 0 <= x && x <= AreaSize && 0 <= y && y <= AreaSize)) return null;
            if (t.HasValue && !(0 <= t && t < _masterData.GamePeriod)) return null;

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock($"move_{idx}", userId, MoveTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= _masterData.GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            var time = t ?? now;
            if (time < now)
            {
                time = now;
            }

            var db = _redis.GetDatabase();
            var res = (RedisResult[])_runMoveScript.Evaluate(db, new { userId, idx, x, y, now, time });
            if (res.Length == 0)
            {
                throw new Exception("move time error");
            }

            Debug.Assert(res.Length % 3 == 0);
            var r = new List<object>();
            for (var i = 0; i < res.Length; i += 3)
            {
                r.Add(new
                {
                    x = double.Parse((string)res[i], NumberStyles.Float),
                    y = double.Parse((string)res[i+1], NumberStyles.Float),
                    t = int.Parse((string)res[i+2]),
                });
            }
            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["now"] = now,
                ["move"] = r,
            });
        }

        private byte[]? RunResources(string rawUrl)
        {
            // /api/resources/([0-9a-z]+)/<ids>
            Debug.Assert(rawUrl.StartsWith(ResourcesEndpoint, StringComparison.Ordinal));
            var param = rawUrl.Substring(ResourcesEndpoint.Length).Split('/');
            if (param.Length != 2) return null;

            var token = param[0];
            if (!ReToken.IsMatch(param[0])) return null;
            var resourceIds = new List<int>();
            foreach (var s in param[1].Split("-"))
            {
                if (!ReNum.IsMatch(s)) return null;
                resourceIds.Add(int.Parse(s));
                if (resourceIds.Count > MaxNumGetResources) return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "too_many_ids",
                });
            }

            var userId = GetUserId(token);
            if (userId == null) return null;

            var nowNullable = WaitUnlock("resources", userId, ResourcesTimeLimit);
            if (!nowNullable.HasValue)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "error_time_limit",
                });
            }

            var now = nowNullable.Value;
            if (now < 0) return null;
            if (now >= _masterData.GamePeriod)
            {
                return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["status"] = "game_finished",
                });
            }

            var resources = new List<MasterData.Resource>();
            foreach (var r in resourceIds.Select(_masterData.GetResource))
            {
                if (r == null || now + ResourceTime <= r.T0)
                {
                    return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                    {
                        ["status"] = "invalid_resource_id",
                    });
                }

                resources.Add(r);
            }

            var db = _redis.GetDatabase();
            var amounts = db.HashGet("amount_hash", resourceIds.Select(id => new RedisValue($"{userId}_{id}")).ToArray());
            Debug.Assert(resources.Count == amounts.Length);

            var resourceData = new List<object>();
            for (var i = 0; i < resources.Count; i++)
            {
                var r = resources[i];
                var amount = amounts[i].IsNull ? 0.0 : (double) amounts[i];
                resourceData.Add(new
                {
                    id = r.Id,
                    x = r.X,
                    y = r.Y,
                    t0 = r.T0,
                    t1 = r.T1,
                    type = r.Type,
                    weight = r.Weight,
                    amount = amount,
                });
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["status"] = "ok",
                ["resource"] = resourceData,
            });
        }

        private void RunSse(HttpListenerResponse response, string rawUrl)
        {
            // /event/([0-9a-z]+)
            Debug.Assert(rawUrl.StartsWith(EventEndpoint, StringComparison.Ordinal));
            var token = rawUrl.Substring(EventEndpoint.Length);

            var connectTime = GetTime() - _masterData.GameStartTime;
            if (connectTime < 0)
            {
                response.StatusCode = (int)HttpStatusCode.Forbidden;
                response.Close();
                return;
            }

            var userId = GetUserId(token);
            if (userId == null)
            {
                response.StatusCode = (int)HttpStatusCode.NotFound;
                response.Close();
                return;
            }

            response.AppendHeader("Content-Type", "text/event-stream");
            response.AppendHeader("X-Accel-Buffering", "no"); // to avoid buffering in nginx

            if (connectTime >= _masterData.GamePeriod)
            {
                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["type"] = "game_finished",
                }));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                return;
            }

            if (GetSetTimeLimit("SSE", userId, connectTime, SseTimeLimit) >= 0)
            {
                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["type"] = "error_time_limit",
                }));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                return;
            }

            var db = _redis.GetDatabase();
            db.Publish(userId, $"C{connectTime}");

            Console.WriteLine("start SSE");

            var ch = Channel.CreateUnbounded<byte[]>();
            var sub = _redis.GetSubscriber().Subscribe(userId);
            try
            {
                sub.OnMessage(message => ch.Writer.WriteAsync(message.Message).AsTask().Wait());

                Thread.Sleep(500);
                var subStartTime = GetTime() - _masterData.GameStartTime;
                if (subStartTime >= _masterData.GamePeriod)
                {
                    response.OutputStream.Write(SseHeader);
                    response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                    {
                        ["type"] = "game_finished",
                    }));
                    response.OutputStream.Write(SseFooter);
                    response.OutputStream.Flush();
                    return;
                }

                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(GetGameSse(db, userId, subStartTime));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                var lastResourceTime = subStartTime;

                for (;;)
                {
                    var cancel = new CancellationTokenSource();
                    cancel.CancelAfter(SseCancelTime);
                    try
                    {
                        var task = ch.Reader.ReadAsync(cancel.Token).AsTask();
                        task.Wait(cancel.Token);
                        var msg = task.Result;
                        switch (msg[0])
                        {
                            case (byte)'M':
                            {
                                var s = GetMoveSse(msg, subStartTime);
                                if (s != null)
                                {
                                    response.OutputStream.Write(SseHeader);
                                    response.OutputStream.Write(s);
                                    response.OutputStream.Write(SseFooter);
                                    response.OutputStream.Flush();
                                }
                                break;
                            }
                            case (byte)'R':
                            {
                                response.OutputStream.Write(SseHeader);
                                response.OutputStream.Write(msg.AsSpan()[1..]);
                                response.OutputStream.Write(SseFooter);
                                response.OutputStream.Flush();
                                break;
                            }
                            case (byte)'C':
                            {
                                if (long.Parse(Encoding.UTF8.GetString(msg.AsSpan()[1..])) != connectTime)
                                {
                                    response.OutputStream.Write(SseHeader);
                                    response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                                    {
                                        ["type"] = "disconnected",
                                    }));
                                    response.OutputStream.Write(SseFooter);
                                    response.OutputStream.Flush();
                                    return;
                                }
                                break;
                            }
                            default:
                                throw new Exception(Encoding.UTF8.GetString(msg));
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        response.OutputStream.Write(SsePing);
                        response.OutputStream.Flush();
                    }

                    var now = GetTime() - _masterData.GameStartTime;
                    if (now >= _masterData.GamePeriod)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                        {
                            ["type"] = "game_finished",
                        }));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                        return;
                    }

                    var resourceData = new List<MasterData.Resource>();
                    foreach (var r in _masterData.Resources)
                    {
                        if (lastResourceTime <= r.T0 - ResourceTime && r.T0 - ResourceTime < now)
                        {
                            resourceData.Add(r);
                        }
                    }

                    lastResourceTime = now;

                    if (resourceData.Count > 0)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                        {
                            ["type"] = "resource",
                            ["resource"] = resourceData,
                            ["now"] = GetTime() - _masterData.GameStartTime,
                        }));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                    }
                }
            }
            catch (Exception)
            {
                sub.Unsubscribe();
                throw;
            }
        }

        private void RunAdminSse(HttpListenerResponse response)
        {
            response.AppendHeader("Content-Type", "text/event-stream");
            response.AppendHeader("X-Accel-Buffering", "no"); // to avoid buffering in nginx

            var ch = Channel.CreateUnbounded<byte[]>();
            var sub = _redis.GetSubscriber().Subscribe("admin");
            try
            {
                sub.OnMessage(message => ch.Writer.WriteAsync(message.Message).AsTask().Wait());

                Thread.Sleep(500);
                var subStartTime = GetTime() - _masterData.GameStartTime - AdminDelay;
                if (subStartTime >= _masterData.GamePeriod)
                {
                    response.OutputStream.Write(SseHeader);
                    response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                    {
                        ["type"] = "game_finished",
                    }));
                    response.OutputStream.Write(SseFooter);
                    response.OutputStream.Flush();
                    return;
                }

                response.OutputStream.Write(SseHeader);
                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                {
                    ["type"] = "game",
                    ["now"] = subStartTime,
                    ["game_period"] = _masterData.GamePeriod,
                    ["agent"] = Array.Empty<int>(),
                    ["resource"] = GetResourceData(subStartTime),
                    ["userId"] = "admin",
                }));
                response.OutputStream.Write(SseFooter);
                response.OutputStream.Flush();
                var lastResourceTime = subStartTime;

                for (;;)
                {
                    var cancel = new CancellationTokenSource();
                    cancel.CancelAfter(SseCancelTime);
                    try
                    {
                        var task = ch.Reader.ReadAsync(cancel.Token).AsTask();
                        task.Wait(cancel.Token);
                        var msg = task.Result;
                        switch (msg[0])
                        {
                            case (byte)'M':
                            {
                                var s = GetMoveSse(msg, subStartTime);
                                if (s != null)
                                {
                                    response.OutputStream.Write(SseHeader);
                                    response.OutputStream.Write(s);
                                    response.OutputStream.Write(SseFooter);
                                    response.OutputStream.Flush();
                                }
                                break;
                            }
                            case (byte)'R':
                            {
                                response.OutputStream.Write(SseHeader);
                                response.OutputStream.Write(msg.AsSpan()[1..]);
                                response.OutputStream.Write(SseFooter);
                                response.OutputStream.Flush();
                                break;
                            }
                            case (byte)'U':
                            {
                                response.OutputStream.Write(SseHeader);
                                response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                                {
                                    ["type"] = "update",
                                }));
                                response.OutputStream.Write(SseFooter);
                                response.OutputStream.Flush();
                                break;
                            }
                            default:
                                throw new Exception(Encoding.UTF8.GetString(msg));
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        response.OutputStream.Write(SsePing);
                        response.OutputStream.Flush();
                    }

                    var now = GetTime() - _masterData.GameStartTime - AdminDelay;
                    if (now >= _masterData.GamePeriod)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                        {
                            ["type"] = "game_finished",
                        }));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                        return;
                    }

                    var resourceData = new List<MasterData.Resource>();
                    foreach (var r in _masterData.Resources)
                    {
                        if (lastResourceTime <= r.T0 - ResourceTime && r.T0 - ResourceTime < now)
                        {
                            resourceData.Add(r);
                        }
                    }

                    lastResourceTime = now;

                    if (resourceData.Count > 0)
                    {
                        response.OutputStream.Write(SseHeader);
                        response.OutputStream.Write(JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
                        {
                            ["type"] = "resource",
                            ["resource"] = resourceData,
                            ["now"] = GetTime() - _masterData.GameStartTime,
                        }));
                        response.OutputStream.Write(SseFooter);
                        response.OutputStream.Flush();
                    }
                }
            }
            catch (Exception)
            {
                sub.Unsubscribe();
                throw;
            }
        }

        private byte[] GetGameSse(IDatabase db, string userId, long now)
        {
            var res = (RedisResult[])_runGameScript.Evaluate(db, new { userId, now });
            Debug.Assert(res.Length == 2);

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["type"] = "game",
                ["now"] = now,
                ["game_period"] = _masterData.GamePeriod,
                ["agent"] = ConvertAgentData((RedisResult[]) res[0]),
                ["resource"] = GetResourceData(now),
                ["userId"] = userId,
            });
        }

        private static List<object> ConvertAgentData(RedisResult[] res0)
        {
            var agentData = new List<object>();
            foreach (var a in res0)
            {
                var b = (RedisResult[])a;
                Debug.Assert(b.Length % 3 == 0);
                var r = new List<object>();
                for (var i = 0; i < b.Length; i += 3)
                {
                    r.Add(new
                    {
                        x = double.Parse((string)b[i], NumberStyles.Float),
                        y = double.Parse((string)b[i+1], NumberStyles.Float),
                        t = int.Parse((string)b[i+2]),
                    });
                }
                agentData.Add(new { move = r });
            }

            return agentData;
        }

        private List<MasterData.Resource> GetResourceData(long now)
        {
            var resourceData = new List<MasterData.Resource>();
            foreach (var r in _masterData.Resources)
            {
                if (r.T1 < now) continue;
                if (now + ResourceTime <= r.T0) continue;
                resourceData.Add(r);
            }

            return resourceData;
        }

        private int GetNextResource(long now)
        {
            var minT0 = int.MaxValue;
            foreach (var r in _masterData.Resources)
            {
                if (now + ResourceTime <= r.T0)
                {
                    minT0 = Math.Min(minT0, r.T0);
                }
            }

            return minT0 == int.MaxValue ? -1 : minT0;
        }

        private static List<object> GetOwnedResource(RedisResult[] res1)
        {
            if (res1.Length == 0) return new List<object>();
            Debug.Assert(res1.Length == 3);
            return new List<object>
            {
                new {type = "A", amount = double.Parse((string) res1[0], NumberStyles.Float)},
                new {type = "B", amount = double.Parse((string) res1[1], NumberStyles.Float)},
                new {type = "C", amount = double.Parse((string) res1[2], NumberStyles.Float)},
            };
        }

        private static long ReadLongFromSpan(Span<byte> msg, ref int p)
        {
            long r = 0;
            while (p < msg.Length)
            {
                var c = msg[p];
                if (c == (byte) ' ')
                {
                    ++p;
                    break;
                }

                if (!(0x30 <= c && c <= 0x39)) throw new Exception("ReadLongFromSpan error");

                r = 10 * r + (c - 0x30);
                ++p;
            }

            return r;
        }

        private static double ReadDoubleFromSpan(Span<byte> msg, ref int p)
        {
            var p0 = p;
            while (p < msg.Length)
            {
                if (msg[p] == (byte) ' ')
                {
                    var r = double.Parse(Encoding.UTF8.GetString(msg[p0..p]), NumberStyles.Float);
                    ++p;
                    return r;
                }

                ++p;
            }

            return double.Parse(Encoding.UTF8.GetString(msg[p0..]), NumberStyles.Float);
        }

        private static byte[]? GetMoveSse(byte[] msg, long subStartTime)
        {
            var s = msg.AsSpan();
            var p = 1;
            var idx = ReadLongFromSpan(s, ref p);
            var moveTime = ReadLongFromSpan(s, ref p);
            if (moveTime < subStartTime) return null;

            var r = new List<object>();
            while (p < s.Length)
            {
                var x = ReadDoubleFromSpan(s, ref p);
                var y = ReadDoubleFromSpan(s, ref p);
                var t = ReadLongFromSpan(s, ref p);
                r.Add(new { x, y, t });
            }

            return JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object>
            {
                ["type"] = "move",
                ["idx"] = idx,
                ["now"] = moveTime,
                ["move"] = r,
            });
        }

        private int GetSetTimeLimit(string type, string userId, long now, int timeLimit)
        {
            var db = _redis.GetDatabase();
            var field = $"{type}_{userId}";
            var r = (string)_runTimeLimitScript.Evaluate(db, new { field, now, timeLimit });
            return r == "ok" ? -1 : int.Parse(r);
        }

        private string? GetUserId(string token)
        {
            var db = _redis.GetDatabase();
            var res = db.HashGet("user_token", token);
            return res.IsNull ? null : res.ToString();
        }

        public static long GetTime()
        {
            return (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) / 10000;
        }
    }
}
