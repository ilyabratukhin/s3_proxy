import logging
import os

from aiohttp import web
import aioboto3

logger = logging.getLogger(__name__)


async def serve_blob(
        request: web.Request,
) -> web.StreamResponse:
    logger.info("start handle")
    env = request.match_info.get('env', None)
    filename = request.match_info.get('filename', None)

    bucket_name = os.getenv("BUCKET_NAME")
    chunk_size = int(os.getenv("CHUNK_SIZE", 69 * 1024))

    blob_s3_key = f"{env}/{filename}"

    session = aioboto3.Session()
    async with session.client("s3") as s3:
        logger.info(f"Serving {bucket_name} {blob_s3_key}")
        try:
            s3_ob = await s3.get_object(Bucket=bucket_name, Key=blob_s3_key)
        except Exception as e:
            logger.error(e)
            return web.Response(status=404)
        ob_info = s3_ob["ResponseMetadata"]["HTTPHeaders"]
        resp = web.StreamResponse(
            headers={
                "Content-Type": ob_info["content-type"],
            }
        )
        logger.info(str(ob_info))
        resp.content_type = ob_info["content-type"]
        resp.content_length = ob_info["content-length"]
        await resp.prepare(request)
        stream = s3_ob["Body"]
        try:
            file_data = await stream.read(chunk_size)
            while file_data:
                logger.debug("write data")
                await resp.write(file_data)
                logger.debug("read data from stream")
                file_data = await stream.read(chunk_size)
        finally:
            stream.close()
    return resp


app = web.Application()
app.add_routes([
    web.get('/{env}/{filename}', serve_blob)
])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("started container")
    web.run_app(app, port=8080)
