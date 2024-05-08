import spacy
import pandas as pd
from langdetect import detect, DetectorFactory, lang_detect_exception
from deep_translator import GoogleTranslator
from tqdm import tqdm


if __name__ == '__main__':

    tags_df = pd.DataFrame(columns=['video_url', 'tag'])
    df = pd.read_csv('./data/videos.csv')

    DetectorFactory.seed = 0

    nlp = spacy.load('ru_core_news_lg')
    tqdm.pandas()

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        text = str(row['title']) + ' ' + str(row['description'])[:4999]
        try:
            lang = detect(text)
        except lang_detect_exception.LangDetectException:
            lang = 'en'
        if lang != 'ru':
            text = GoogleTranslator(source='auto', target='ru').translate(text[:4999])
        doc = nlp(text)
        for ent in doc.ents:
            tags_df.loc[len(tags_df)] = [row['url'], ent.text]

    tags_df.drop_duplicates(inplace=True)
    tags_df.to_csv('./data/tags.csv', index=False)
