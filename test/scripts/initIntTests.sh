docker-compose --file ./test/scripts/docker-compose.yml up -d

if [ $? -eq 0 ]
then
  echo "Successfully started docker"
else
  echo "Could not start docker" >&2
  exit 1
fi

while curl -s http://localhost:4566/health | grep -v "\"initScripts\": \"initialized\""; do
    printf 'SQS is unavailable - sleeping'
    sleep 10s
done

echo "Service is up and running"