import logging
import os
import uuid
from io import BytesIO
from aiohttp import web
import aioboto3

logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("BUCKET_NAME")
S3_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 69 * 1024))


async def serve_blob(
        request: web.Request,
) -> web.StreamResponse:
    logger.info("start handle")
    filename = request.match_info.get('filename', None)

    blob_s3_key = f"{filename}"

    session = aioboto3.Session()
    async with session.client("s3") as s3:
        logger.info(f"Serving {BUCKET_NAME} {blob_s3_key}")
        try:
            s3_ob = await s3.get_object(Bucket=BUCKET_NAME, Key=blob_s3_key)
        except Exception as e:
            logger.error(e)
            return web.Response(status=404, text="Image not found")
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
            file_data = await stream.read(S3_CHUNK_SIZE)
            while file_data:
                logger.debug("write data")
                await resp.write(file_data)
                logger.debug("read data from stream")
                file_data = await stream.read(S3_CHUNK_SIZE)
        finally:
            stream.close()
    return resp


async def upload(request: web.Request) -> web.Response:
    logger.info("start uploading handle")
    try:
        reader = await request.multipart()
    except KeyError:
        return web.Response(status=400)

    field = await reader.next()
    filename = field.filename
    if len(filename.split('.')) < 2:
        return web.Response(status=400, text="Invalid name of file")

    content_type = field.headers['Content-Type']
    if not content_type.startswith('image/'):
        return web.Response(status=400, text='Invalid format of file')

    size = 0
    f = BytesIO()
    while True:
        chunk = await field.read_chunk()  # 8192 bytes by default.
        if not chunk:
            break
        size += len(chunk)
        f.write(chunk)
    f.seek(0)

    session = aioboto3.Session()
    async with session.client("s3") as s3:
        s3_ob = True
        file_type = filename.split('.')[-1]
        try:
            while s3_ob:
                blob_s3_key = f"{uuid.uuid4()}.{file_type}"
                s3_ob = await s3.get_object(Bucket=BUCKET_NAME, Key=blob_s3_key)
        except Exception:  # everything is fine, object was not found
            pass

        logger.info(f"Serving {filename} for {BUCKET_NAME} as {blob_s3_key}")
        try:
            await s3.upload_fileobj(
                f,
                BUCKET_NAME,
                blob_s3_key,
                ExtraArgs={"ContentType": content_type}
            )
        except Exception as e:
            logger.error(f"Unable to s3 upload {filename} to {blob_s3_key}: {e} ({type(e)})")
            return web.Response(text=f'{filename} was not uploaded', status=500)
    return web.Response(text=blob_s3_key)


app = web.Application()
app.add_routes([
    web.post('/upload', upload),
    web.get('/{filename:.*}', serve_blob),

])

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("started container")
    web.run_app(app, port=8080)
