using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync.Repositories
{
    public class JourneysRepository : IJourneysRepository
    {
        private const string ConnString = "Server=(local);Database=MyAppDb;Trusted_Connection=true;";

        private const string GetMaxIdQuery = @"SELECT MAX([JourneyId])
FROM [MyAppDb].[dbo].[Journey]";

        private const string GetMaxDateQuery = @"SELECT MAX([TravelDate])
FROM [MyAppDb].[dbo].[Journey]";

        private const string GetJourneysBetweenIdsQuery = @"SELECT [JourneyId]
      ,[DepartureStation]
      ,[ArrivalStation]
      ,[TravelDate]
      ,[PassengerName]
FROM[MyAppDb].[dbo].[Journey]
WHERE JourneyId BETWEEN @StartId AND @EndId";

        private const string GetJourneysBetweenDatesQuery = @"SELECT [JourneyId]
      ,[DepartureStation]
      ,[ArrivalStation]
      ,[TravelDate]
      ,[PassengerName]
FROM[MyAppDb].[dbo].[Journey]
WHERE TravelDate >= @StartDate
AND TravelDate < @EndDate";

        public async Task<long> GetMaxJourneyIdAsync()
        {
            using (var conn = new SqlConnection(ConnString))
            {
                await conn.OpenAsync();
                using (var command = new SqlCommand(GetMaxIdQuery, conn))
                {
                    return (int)await command.ExecuteScalarAsync();
                }
            }
        }

        public async Task<DateTime> GetMaxJourneyDateAsync()
        {
            using (var conn = new SqlConnection(ConnString))
            {
                await conn.OpenAsync();
                using (var command = new SqlCommand(GetMaxDateQuery, conn))
                {
                    return (DateTime)await command.ExecuteScalarAsync();
                }
            }
        }

        public async Task<IList<Journey>> GetJourneysAsync(long startId, long endId)
        {
            var journeys = new List<Journey>();

            using (var conn = new SqlConnection(ConnString))
            {
                await conn.OpenAsync();
                using (var command = new SqlCommand(GetJourneysBetweenIdsQuery, conn))
                {
                    command.Parameters.Add("StartId", SqlDbType.Int).Value = startId;
                    command.Parameters.Add("EndId", SqlDbType.Int).Value = endId;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var journey = new Journey();

                            journey.JourneyId = (int)reader["JourneyId"];
                            journey.ArrivalStation = reader["ArrivalStation"].ToString();
                            journey.DepartureStation = reader["DepartureStation"].ToString();
                            journey.TravelDate = (DateTime)reader["TravelDate"];
                            journey.PassengerName = reader["PassengerName"].ToString();

                            journeys.Add(journey);
                        }
                    }
                }
            }

            return journeys;
        }

        public async Task<IList<Journey>> GetJourneysAsync(DateTime startDate, DateTime endDate)
        {
            var journeys = new List<Journey>();

            using (var conn = new SqlConnection(ConnString))
            {
                await conn.OpenAsync();
                using (var command = new SqlCommand(GetJourneysBetweenDatesQuery, conn))
                {
                    command.Parameters.Add("StartDate", SqlDbType.DateTime).Value = startDate;
                    command.Parameters.Add("EndDate", SqlDbType.DateTime).Value = endDate;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var journey = new Journey();

                            journey.JourneyId = (int)reader["JourneyId"];
                            journey.ArrivalStation = reader["ArrivalStation"].ToString();
                            journey.DepartureStation = reader["DepartureStation"].ToString();
                            journey.TravelDate = (DateTime)reader["TravelDate"];
                            journey.PassengerName = reader["PassengerName"].ToString();

                            journeys.Add(journey);
                        }
                    }
                }
            }

            return journeys;
        }
    }
}
