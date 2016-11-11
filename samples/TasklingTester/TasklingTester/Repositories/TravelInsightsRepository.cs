using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTester.Repositories
{
    public class TravelInsightsRepository : ITravelInsightsRepository
    {
        private const string ConnString = "Server=(local);Database=MyAppDb;Trusted_Connection=true;";


        private const string InsertCommand = @"INSERT INTO [MyAppDb].[dbo].[TravelInsight]
           ([PassengerName]
           ,[InsightText]
           ,[InsightDate])
     VALUES
           (@PassengerName
           ,@InsightText
           ,@InsightDate)";

        public void Add(IList<TravelInsight> insights)
        {
            using (var conn = new SqlConnection(ConnString))
            {
                conn.Open();
                foreach (var insight in insights)
                {
                    using (var command = new SqlCommand(InsertCommand, conn))
                    {
                        command.Parameters.Add("PassengerName", SqlDbType.VarChar, 100).Value = insight.PassengerName;
                        command.Parameters.Add("InsightText", SqlDbType.VarChar, 1000).Value = insight.InsightText;
                        command.Parameters.Add("InsightDate", SqlDbType.DateTime).Value = insight.InsightDate;
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
