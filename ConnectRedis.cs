using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using System;

namespace FlightTracker
{
    public static class ConnectRedis
    {
        private static ConnectionMultiplexer muxer;

        public static IDatabase GetDatabase()
        {
            if (muxer == null)
            {
                var config = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();

                string host = config["Redis:Host"];
                int port = int.Parse(config["Redis:Port"]);
                string user = config["Redis:User"];
                string password = config["Redis:Password"];

                muxer = ConnectionMultiplexer.Connect(
                    new ConfigurationOptions
                    {
                        EndPoints = { { host, port } },
                        User = user,
                        Password = password
                    }
                );
            }

            return muxer.GetDatabase();
        }
    }
}
