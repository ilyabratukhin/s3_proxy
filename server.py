import logging
import os

from aiohttp import web
import aioboto3

logger = logging.getLogger(__name__)


async def serve_blob(
        request: web.Request,
) -> web.StreamResponse:
    env = request.match_info.get('env', None)
    filename = request.match_info.get('filename', None)

    bucket_name = os.getenv("BUCKET_NAME")
    chunk_size = int(os.getenv("CHUNK_SIZE", 69 * 1024))

    blob_s3_key = f"{env}/{filename}"

    session = aioboto3.Session()
    async with session.client("s3") as s3:
        logger.info(f"Serving {bucket_name} {blob_s3_key}")
        s3_ob = await s3.get_object(Bucket=bucket_name, Key=blob_s3_key)

        ob_info = s3_ob["ResponseMetadata"]["HTTPHeaders"]
        resp = web.StreamResponse(
            headers={
                "CONTENT-DISPOSITION": (
                    f"attachment; filename='{filename}'"
                ),
                "Content-Type": ob_info["content-type"],
            }
        )
        resp.content_type = ob_info["content-type"]
        resp.content_length = ob_info["content-length"]
        await resp.prepare(request)

        async with s3_ob["Body"] as stream:
            file_data = await stream.read(chunk_size)
            while file_data:
                await resp.write(file_data)
                file_data = await stream.read(chunk_size)

    return resp


app = web.Application()
app.add_routes([
    web.get('/{env}/{filename}', serve_blob)
])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("started container")
    web.run_app(app, port=8080)
