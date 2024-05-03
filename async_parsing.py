import asyncio
import json
import pandas as pd
import httpx


async def get_stats_gather(url):
    async with httpx.AsyncClient(timeout=None) as client:
        try:
            response = await client.get(url)
            if response.status_code != 200:
                await asyncio.sleep(1)
                response = await client.get(url)
        except httpx.HTTPStatusError as e:
            print('error', e.response.status_code)
        return response


async def parse():
    videos_df = pd.read_csv('./data/videos.csv')
    authors_df = pd.read_csv('./data/authors.csv')
    comments_df = pd.read_csv('./data/comments.csv')

    videos_urls = list(videos_df['url'])
    authors_urls = list(videos_df['author_id'].unique())

    videos_api_urls = [f'https://rutube.ru/api/video/{url}' for url in videos_urls]
    likes_api_urls = [f'https://rutube.ru/api/numerator/video/{url}/vote' for url in videos_urls]
    comments_api_urls = [f'https://rutube.ru/api/comments/video/{url}' for url in videos_urls]
    authors_api_urls = [f'https://rutube.ru/api/profile/user/{url}' for url in authors_urls]

    info_tasks = []
    for url in videos_api_urls:
        info_tasks.append(get_stats_gather(url))

    info_res = await asyncio.gather(*info_tasks)
    print('video_info_done')

    likes_tasks = []
    for url in likes_api_urls:
        likes_tasks.append(get_stats_gather(url))

    likes_res = await asyncio.gather(*likes_tasks)
    print('likes_done')

    comments_tasks = []
    for url in comments_api_urls:
        comments_tasks.append(get_stats_gather(url))

    comments_res = await asyncio.gather(*comments_tasks)
    print('comments_done')

    authors_tasks = []
    for url in authors_api_urls:
        authors_tasks.append(get_stats_gather(url))

    authors_res = await asyncio.gather(*authors_tasks)
    print('authors_done')

    videos_df['title'] = [r.json().get('title') for r in info_res]
    videos_df['description'] = [r.json().get('description') for r in info_res]
    videos_df['hits'] = [r.json().get('hits', 0) for r in info_res]
    videos_df['duration'] = [r.json().get('duration', 0) for r in info_res]
    videos_df['likes'] = [r.json().get('positive', 0) for r in likes_res]
    videos_df['dislikes'] = [r.json().get('negative', 0) for r in likes_res]
    videos_df['comments_count'] = [r.json().get('comments_count', 0) for r in comments_res]
    videos_df['created_ts'] = [r.json().get('created_ts') for r in info_res]
    videos_df['publication_ts'] = [r.json().get('publication_ts') for r in info_res]
    videos_df['created_ts'].fillna('1970-01-01T00:00:00', inplace=True)
    videos_df['publication_ts'].fillna('1970-01-01T00:00:00', inplace=True)
    videos_df['created_ts'] = pd.to_datetime(videos_df['created_ts'].str.strip(), format='%Y-%m-%dT%H:%M:%S')
    videos_df['publication_ts'] = pd.to_datetime(videos_df['publication_ts'].str.strip(), format='%Y-%m-%dT%H:%M:%S')

    for comments in comments_res:
        try:
            for comment in comments.json()['results']:
                comments_df.loc[len(comments_df.index)] = [comment['id'], comment['video_id'], comment['user']['id'],
                                                           comment['text'].encode('utf-8'), comment['created_ts'],
                                                           comment['likes_number'], comment['dislikes_number'],
                                                           comment['replies_number']]
        except KeyError:
            pass

    print(comment.json()['results']['text'] for comment in comments_res)

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
