using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTester
{
    public class Journey
    {
        public long JourneyId { get; set; }
        public string DepartureStation { get; set; }
        public string ArrivalStation { get; set; }
        public DateTime TravelDate { get; set; }
        public string PassengerName { get; set; }
    }
}
