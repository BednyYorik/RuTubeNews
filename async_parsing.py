import asyncio
import numpy as np
import pandas as pd
import httpx
import datetime
from tqdm import tqdm


async def get_stats_gather(session: httpx.AsyncClient, url: str) -> httpx.Response:
    response = None
    try:
        response = await session.get(url)
        while response.status_code == 503:
            await asyncio.sleep(1)
            response = await session.get(url)
    except httpx.HTTPStatusError as e:
        print('error', e.response.status_code)
    return response


async def main(video_urls, likes_urls, comments_urls, authors_urls):
    info_res = []
    likes_res = []
    comments_res = []
    authors_res = []

    async with httpx.AsyncClient(timeout=httpx.Timeout(30)) as session:
        tasks = np.array([get_stats_gather(session, url) for url in video_urls])
        for chunk_tasks in tqdm(np.array_split(tasks, 100)):
            info_res.extend(await asyncio.gather(*chunk_tasks))
        print('info_res done')
        tasks = np.array([get_stats_gather(session, url) for url in likes_urls])
        for chunk_tasks in tqdm(np.array_split(tasks, 100)):
            likes_res.extend(await asyncio.gather(*chunk_tasks))
        print('likes_res done')
        tasks = np.array([get_stats_gather(session, url) for url in comments_urls])
        for chunk_tasks in tqdm(np.array_split(tasks, 100)):
            comments_res.extend(await asyncio.gather(*chunk_tasks))
        print('comments_res done')
        tasks = np.array([get_stats_gather(session, url) for url in authors_urls])
        for chunk_tasks in tqdm(np.array_split(tasks, 100)):
            authors_res.extend(await asyncio.gather(*chunk_tasks))
        print('authors_res done')

    return info_res, likes_res, comments_res, authors_res


if __name__ == '__main__':

    videos_df = pd.DataFrame(columns=['url', 'title', 'description', 'hits', 'duration', 'likes', 'dislikes',
                                      'comments_count', 'author_id', 'created_ts', 'publication_ts'])

    authors_df = pd.DataFrame(columns=['id', 'channel_name', 'channel_description', 'followers',
                                       'is_official_channel'])

    comments_df = pd.DataFrame(columns=['id', 'video_id', 'author_id', 'text', 'created_ts', 'likes', 'dislikes',
                                        'replies_count'])

    i = 1
    flag = True
    while flag:
        videos = httpx.get(f'https://rutube.ru/api/video/category/8/?page={i}')
        videos_json = videos.json()
        for video in videos_json['results']:
            if video['publication_ts'] is not None:
                if (datetime.datetime.strptime(video['publication_ts'], '%Y-%m-%dT%H:%M:%S') >
                        datetime.datetime(2024, 5, 1, 0, 0, 0)):
                    videos_df.loc[len(videos_df.index), ['url', 'author_id']] = [video['id'], video['author']['id']]
                else:
                    flag = False
        i += 1
    print('get list videos done')

    videos_urls = list(videos_df['url'])
    channels_urls = list(videos_df['author_id'].unique())
    videos_api_urls = [f'https://rutube.ru/api/video/{url}' for url in videos_urls]
    likes_api_urls = [f'https://rutube.ru/api/numerator/video/{url}/vote' for url in videos_urls]
    comments_api_urls = [f'https://rutube.ru/api/comments/video/{url}' for url in videos_urls]
    authors_api_urls = [f'https://rutube.ru/api/profile/user/{url}' for url in channels_urls]

    info_res, likes_res, comments_res, authors_res = asyncio.run(main(videos_api_urls, likes_api_urls,
                                                                      comments_api_urls, authors_api_urls))

    videos_df['title'] = [r.json().get('title') for r in info_res]
    videos_df['description'] = [r.json().get('description') for r in info_res]
    videos_df['hits'] = [r.json().get('hits', 0) for r in info_res]
    videos_df['duration'] = [r.json().get('duration', 0) for r in info_res]
    videos_df['likes'] = [r.json().get('positive', 0) for r in likes_res]
    videos_df['dislikes'] = [r.json().get('negative', 0) for r in likes_res]
    videos_df['comments_count'] = [r.json().get('comments_count', 0) for r in comments_res]
    videos_df['created_ts'] = [r.json().get('created_ts') for r in info_res]
    videos_df['publication_ts'] = [r.json().get('publication_ts') for r in info_res]
    videos_df.fillna({'created_ts': '1970-01-01T00:00:00'}, inplace=True)
    videos_df.fillna({'publication_ts': '1970-01-01T00:00:00'}, inplace=True)
    videos_df['created_ts'] = pd.to_datetime(videos_df['created_ts'].str.strip(), format='%Y-%m-%dT%H:%M:%S')
    videos_df['publication_ts'] = pd.to_datetime(videos_df['publication_ts'].str.strip(), format='%Y-%m-%dT%H:%M:%S')

    for comments in comments_res:
        try:
            for comment in comments.json()['results']:
                comments_df.loc[len(comments_df.index)] = [comment['id'], comment['video_id'], comment['user']['id'],
                                                           comment['text'], comment['created_ts'],
                                                           comment['likes_number'], comment['dislikes_number'],
                                                           comment['replies_number']]
        except KeyError:
            pass

    authors_df['id'] = [r.json().get('id') for r in authors_res]
    authors_df['channel_name'] = [r.json().get('name', None) for r in authors_res]
    authors_df['channel_description'] = [r.json().get('description', None) for r in authors_res]
    authors_df['followers'] = [r.json().get('subscribers_count', 0) for r in authors_res]
    authors_df['is_official_channel'] = [r.json().get('is_official', 0) for r in authors_res]

    videos_df.to_csv('./data/videos.csv', index=False)
    comments_df.to_csv('./data/comments.csv', index=False)
    authors_df.to_csv('./data/authors.csv', index=False)
