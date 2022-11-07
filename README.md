# S3 PROXY

## Development

Go to the application folder, copy `.env` file, add Bucket name to .env and run docker cluster

```sh
cp .env.sample .env
WEB_PORT=5050 docker-compose up -d --build
```