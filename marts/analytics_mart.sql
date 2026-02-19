/*Создадим одну большую таблицу fct_flights_mart, в которой будут поля из 
tickets_raw, 
ticket_flights_raw, 
flights, 
bookings_raw.
*/

CREATE TABLE startde_student_data.marija_shkurat_wrn7887_fct_flights_mart(
	--flights
	flight_id 			UInt32,
    flight_no 			String,
    scheduled_departure DateTime,
    scheduled_arrival 	DateTime,
    departure_airport 	String,
    arrival_airport 	String,
    status 				String,
    aircraft_code 		String,
    actual_departure 	DateTime,
    actual_arrival 		DateTime,
    
    --ticket_flights_raw
	fare_conditions 	String,
	amount 				String,

	--tickets_raw
	ticket_no 			String,
	book_ref 			String,
	passenger_id 		String,
	passenger_name 		String,
	contact_data 		String,
	
	--bookings_raw
	book_date 			String NOT NUll,
	total_amount 		String NOT NULL
)
ENGINE = MergeTree
ORDER BY (ticket_no, flight_id)

TRUNCATE TABLE startde_student_data.marija_shkurat_wrn7887_fct_flights_mart;

INSERT INTO startde_student_data.marija_shkurat_wrn7887_fct_flights_mart
SELECT 
	--flights
	f.flight_id::UInt32 			, --UInt32
    f.flight_no::String 			, --String
    f.scheduled_departure::DateTime , --DateTime
    f.scheduled_arrival::DateTime 	, --DateTime
    f.departure_airport::String 	, --String
    f.arrival_airport::String 	, --String
    f.status::String 				, --String
    f.aircraft_code::String 		, --String
    f.actual_departure::Nullable(DateTime) 	, --DateTime
    f.actual_arrival::Nullable(DateTime) 		,--DateTime
    
    --ticket_flights_raw
	tf.fare_conditions::String,
	tf.amount::String,
	
	--tickets_raw
	t.ticket_no::String,
	t.book_ref::String,
	t.passenger_id::String,
	t.passenger_name::String,
	t.contact_data::String,
	
	--bookings_raw
	b.book_date::String,
	b.total_amount::String 
FROM startde_student_data.marija_shkurat_wrn7887_flights f
LEFT JOIN startde_student_data.marija_shkurat_wrn7887_ticket_flights_raw tf  ON f.flight_id::String = tf.flight_id
LEFT JOIN startde_student_data.marija_shkurat_wrn7887_tickets_raw t          ON tf.ticket_no = t.ticket_no
LEFT JOIN startde_student_data.marija_shkurat_wrn7887_bookings_raw b         ON t.book_ref = b.book_ref;
