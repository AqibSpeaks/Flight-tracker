using System;
using StackExchange.Redis;

namespace FlightTracker
{
    class Program
    {
        static void Main(string[] args)
        {
            IDatabase db = ConnectRedis.GetDatabase();

            // Test Redis
            db.StringSet("foo", "bar");
            RedisValue result = db.StringGet("foo");
            Console.WriteLine($"Test value from Redis: {result}"); // >>> bar

            // Here you can add your flight tracker logic to store/read flight data
        }
    }
}
