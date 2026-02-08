-- Fails if booking_status contains unexpected values
-- Adjust allowed list as needed for your business rules
select *
from {{ ref('silver_bookings') }}
where upper(booking_status) not in ('CONFIRMED', 'CANCELLED', 'PENDING', 'COMPLETED')
