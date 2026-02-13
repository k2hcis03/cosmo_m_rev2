from typing import Annotated
from pathlib import Path

from litestar import Litestar, MediaType, post
from litestar.datastructures import UploadFile
from litestar.enums import RequestEncodingType
from litestar.params import Body


# 펌웨어 업로드 디렉토리
# uvicorn app:app --host 172.30.1.100 --port 9002 --reload 실행 명령어
# uvicorn app:app --host 192.168.100.10 --port 9002 --reload 실행 명령어

UPLOAD_DIR = Path("/home/pi/Projects/cosmo-m/firmware")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@post(path="/upload", media_type=MediaType.JSON)
async def upload(
    data: Annotated[UploadFile, Body(media_type=RequestEncodingType.MULTI_PART)],
) -> dict:
    # 펌웨어 업로드 디렉토리에 파일 저장
    content = await data.read()
    filename = data.filename

    dest = UPLOAD_DIR / filename
    with dest.open("wb") as f:
        f.write(content)

    return {
        "status": "OK",
        "filename": filename,
        "size": len(content),
    }


app = Litestar(route_handlers=[upload])
