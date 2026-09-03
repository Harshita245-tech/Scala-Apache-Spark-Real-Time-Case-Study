case class Customer(
  customer_id: String,
  name: String,
  age: Int,
  city: String
)

case class Booking(
  booking_id: String,
  customer_id: String,
  booking_type: String,
  service_id: String,
  booking_date: String,
  amount: Double,
  status: String
)

case class Payment(
  payment_id: String,
  booking_id: String,
  payment_date: String,
  payment_method: String,
  payment_amount: Double,
  payment_status: String
)

case class Cancellation(
  cancellation_id: String,
  booking_id: String,
  customer_id: String,
  cancellation_date: String,
  reason: String,
  refund_amount: Double
)

case class Hotel(
  hotel_id: String,
  hotel_name: String,
  city: String,
  room_type: String,
  price_per_night: Double
)

case class Flight(
  flight_id: String,
  airline: String,
  source: String,
  destination: String,
  seat_capacity: Int
)

case class Bus(
  bus_id: String,
  operator: String,
  source: String,
  destination: String,
  seat_capacity: Int
)
