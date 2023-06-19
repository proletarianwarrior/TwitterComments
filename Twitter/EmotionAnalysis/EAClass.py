# -*- coding: utf-8 -*-
# @Time : 2023/6/15 18:23
# @Author : DanYang
# @File : EAClass.py
# @Software : PyCharm
import json
from collections import Counter

from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import numpy as np
from translate import Translator
from scipy.special import softmax

MODEL = "cardiffnlp/xlm-twitter-politics-sentiment"

tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)


def translate_data(text, target_language):
    translator = Translator(to_lang=target_language)
    translated_text = translator.translate(text)
    return translated_text


def get_max_score(text, language):
    n_text = text if language == "en" else translate_data(text, "en")
    encoded_input = tokenizer(n_text, return_tensors='pt')
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    labels = model.config.id2label
    return [(labels[j], i) for j, i in enumerate(scores)]


def search_difference(file_name):
    with open(f"../Crawler/{file_name}.json", 'r') as file:
        datas = json.load(file)
    articles = [list(i.values())[0] for i in datas]
    na = Counter(articles)
    print(len(na))
    return list(na.keys())


if __name__ == '__main__':
    mean_score = {
        'Negative': 0,
        'Neutral': 0,
        'Positive': 0
    }
    for year in [str(i) for i in range(2013, 2024)]:
        for i in search_difference(year):
            result = get_max_score(i, 'zh')
            for j in result:
                mean_score[j[0]] += j[1]
        total_score = sum(mean_score.values())
        for i in mean_score.keys():
            mean_score[i] /= total_score
        print(mean_score)
        try:
            with open("result.json", 'r') as file:
                data = json.load(file)
        except json.JSONDecodeError:
            data = []
        with open("result.json", 'w') as file:
            data.append({year: mean_score})
            json.dump(data, file, indent=3)
