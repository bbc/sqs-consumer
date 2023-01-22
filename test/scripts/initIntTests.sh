docker-compose --file ./test/scripts/docker-compose.yml up -d

if [ $? -eq 0 ]
then
  echo "Successfully started docker"
else
  echo "Could not start docker" >&2
  exit 1
fi