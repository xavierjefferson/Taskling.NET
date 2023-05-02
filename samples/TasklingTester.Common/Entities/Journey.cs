namespace TasklingTester.Common.Entities
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
