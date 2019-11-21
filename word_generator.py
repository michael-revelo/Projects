from nltk.corpus import wordnet as wn
from googletrans import Translator
from indic_transliteration import sanscript
from indic_transliteration.sanscript import SchemeMap, SCHEMES, transliterate
from hangul_romanize import Transliter
from hangul_romanize.rule import academic
import numpy as np
import pinyin as p
import requests
import json
import pyttsx3

words = list(wn.all_lemma_names())
engine = pyttsx3.init()
english_translationclear = ''
spanish_translation = ''
german_translation = ''
hindi_translation = ''
chinese_translation = ''
korean_translation = ''

def helper():	
	np.random.choice(words)
	main()
	
def main():
	word = np.random.choice(words)
	syns = wn.synsets(word)
	desc = syns[0].definition()
	translator = Translator()

	if "_" in word or "-" in word:
		helper()
	else:
		english_translation = english(word, desc)
		spanish_translation = spanish(word, desc)
		german_translation = german(word, desc)
		hindi_translation = hindi(word, desc)
		chinese_translation = chinese(word, desc)
		korean_translation = korean(word, desc)
		
		all_translations = (english_translation + '\n\n' + 
			  				spanish_translation + '\n\n' +
			  				german_translation + '\n\n' +
			  				hindi_translation + '\n\n' +
			  				chinese_translation + '\n\n' +
			  				korean_translation)

		message_json = {'text': all_translations}
		slack_message(message_json)

		'''engine.say(word)
		engine.say(translator.translate(word, dest='es').text)
		engine.say(translator.translate(word, dest='de').text)
		engine.runAndWait()'''


def english(n, m):
	english_translation = "English - " + n + " - " + m
	return english_translation

def spanish(n, m):
	translator = Translator()
	spanish = translator.translate(n, dest='es')
	spanish_desc = translator.translate(m, dest='es')
	spanish_translation = "Spanish - " + spanish.text + " - " + spanish_desc.text
	return spanish_translation
	

def german(n, m):
	translator = Translator()
	german = translator.translate(n, dest='de')
	german_desc = translator.translate(m, dest='de')
	german_translation = "German - " + german.text + " - " + german_desc.text
	return german_translation

def hindi(n, m):
	translator = Translator()
	hindi = translator.translate(n, dest='hi')
	hindi_desc = translator.translate(m, dest='hi')
	hindi_trans = transliterate(hindi.text, sanscript.DEVANAGARI, sanscript.HK)
	hindi_desc_trans = transliterate(hindi_desc.text, sanscript.DEVANAGARI, sanscript.HK)
	hindi_translation = "Hindi - " + hindi_trans + " - " + hindi_desc_trans
	return hindi_translation

def chinese(n, m):
	translator = Translator()
	chinese = translator.translate(n, dest='zh-cn')
	chinese_desc = translator.translate(m, dest='zh-cn')
	chinese_desc_spaced = " ".join(chinese_desc.text)
	chinese_translation = "Chinese - " + p.get(chinese.text) + " - " + p.get(chinese_desc_spaced)
	return chinese_translation

def korean(n, m):
	translator = Translator()
	transliter = Transliter(academic)
	korean = translator.translate(n, dest='ko')
	korean_desc = translator.translate(m, dest='ko')
	korean_translation = "Korean - " + transliter.translit(korean.text) + " - " + transliter.translit(korean_desc.text)
	return korean_translation

def slack_message(message):
	WEBHOOK_URL = "https://hooks.slack.com/services/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"
	requests.post(WEBHOOK_URL, data=json.dumps(message),headers={'Content-Type': 'application/json'})

main()

