import asyncio
import pandas as pd
import httpx

videos_df = pd.read_csv('./data/videos.csv')
authors_df = pd.read_csv('./data/authors.csv')
comments_df = pd.read_csv('./data/comments.csv')

videos_urls = list(videos_df['url'])

videos_api_urls = [f'https://rutube.ru/api/video/{url}' for url in videos_urls]
likes_api_urls = [f'https://rutube.ru/api/numerator/video/{url}/vote' for url in videos_urls]
comments_api_urls = [f'https://rutube.ru/api/comments/video/{url}' for url in videos_urls]


async def get_stats(urls):

    async with httpx.AsyncClient(timeout=None) as client:
        results = []
        for url in urls:
            try:
                response = await client.get(url)
                results.append(response)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    await asyncio.sleep(1)
                    response = await client.get(url)
                    results.append(response)
                else:
                    results.append(None)
                    raise
        return results


async def parse():
    info_res = await get_stats(videos_api_urls)
    likes_res = await get_stats(likes_api_urls)
    comments_res = await get_stats(comments_api_urls)

    videos_df['title'] = [r.json().get('title', None) for r in info_res]
    videos_df['description'] = [r.json().get('description', None) for r in info_res]
    videos_df['hits'] = [r.json().get('hits', 0) for r in info_res]
    videos_df['duration'] = [r.json().get('duration', 0) for r in info_res]
    videos_df['likes'] = [r.json().get('positive', 0) for r in likes_res]
    videos_df['dislikes'] = [r.json().get('negative', 0) for r in likes_res]
    videos_df['comments_count'] = [r.json().get('comments_count', 0) for r in comments_res]
    videos_df['author_id'] = [r.json()['author']['id'] for r in info_res]
    videos_df['created_ts'] = [r.json().get('created_ts', 0) for r in info_res]
    videos_df['publication_ts'] = [r.json().get('publication_ts', 0) for r in info_res]

    for comments in comments_res:
        try:
            for comment in comments.json()['results']:
                comments_df.loc[len(comments_df.index)] = [comment['id'], comment['video_id'], comment['user']['id'],
                                                           comment['text'], comment['created_ts'],
                                                           comment['likes_number'], comment['dislikes_number'],
                                                           comment['replies_number']]
        except KeyError:
            pass

    authors_urls = list(videos_df['author_id'].drop_duplicates())
    authors_api_urls = [f'https://rutube.ru/api/profile/user/{url}' for url in authors_urls]

    authors_res = await get_stats(authors_api_urls)

    authors_df['id'] = [r.json().get('id') for r in authors_res]
    authors_df['channel_name'] = [r.json().get('name', None) for r in authors_res]
    authors_df['channel_description'] = [r.json().get('description', None) for r in authors_res]
    authors_df['followers'] = [r.json().get('subscribers_count', 0) for r in authors_res]
    authors_df['is_official_channel'] = [r.json().get('is_official', 0) for r in authors_res]

    videos_df.to_csv('./data/videos.csv', index=False)
    comments_df.to_csv('./data/comments.csv', index=False)
    authors_df.to_csv('./data/authors.csv', index=False)

if __name__ == '__main__':
    asyncio.run(parse())
