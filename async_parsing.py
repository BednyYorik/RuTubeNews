import asyncio
import pandas as pd
import httpx

videos_df = pd.read_csv('./videos.csv')
videos_urls = list(videos_df['url'])
videos_api_urls = [f'https://rutube.ru/api/numerator/video/{url}/vote' for url in videos_urls ]


async def get_video_stats():

    async with httpx.AsyncClient(timeout=None) as client:
        results = []
        for url in videos_api_urls:
            try:
                response = await client.get(url)
                results.append(response)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    await asyncio.sleep(1)
                    results.append(None)
                else:
                    raise
        return results


res = asyncio.run(get_video_stats())

videos_df = videos_df.assign(
    positive=[r.json().get('positive', 0) for r in res],
    negative=[r.json().get('negative', 0) for r in res]
)

print(videos_df)

videos_df.to_csv('./videos.csv', index=False)
