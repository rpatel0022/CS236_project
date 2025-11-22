docker run -d \
  --name hotel_reservations \
  -e POSTGRES_USER=cs236_user \
  -e POSTGRES_PASSWORD=cs236_pass \
  -e POSTGRES_DB=hotel_reservations \
  -p 5432:5432 \
  postgres:16

  