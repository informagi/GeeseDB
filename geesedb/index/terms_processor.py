import os
import nltk.data
import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
from syntok.tokenizer import Tokenizer
from geesedb.index.cython.test import process

import time


class TermsProcessor:
    """
    Performs stemming and stop words.
    Uses nltk by default with the process method.
    Inputs the whole document as a string.
    """

    def __init__(self, arguments):
        if arguments['stemmer'] == 'porter':
            self.stemmer = PorterStemmer()
        elif arguments['stemmer'] == 'snowball':
            self.stemmer = SnowballStemmer(arguments['language'])
        else:
            IOError('Please input a valid stemmer')
        self.file_path = arguments['nltk_path']
        self.delete_chars = arguments['delete_chars']
        self.delete_chars_str = ''.join(self.delete_chars)
        self.tokenization_method = arguments['tokenization_method']
        if arguments['create_nltk_data']:
            self.download_nltk_data()
        if arguments['stop_words'] is None:
            self.stop_words = set(self.delete_chars)
        elif arguments['stop_words'] == 'nltk':
            self.stop_words = set(stopwords.words(arguments['language']))
            self.stop_words.update(self.delete_chars)
        elif arguments['stop_words'] == 'lucene':
            self.stop_words = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
                               "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
                               "these", "they", "this", "to", "was", "will", "with"}
            self.stop_words.update(self.delete_chars)
        else:
            IOError('Please select a valid option')
        self.nlp = spacy.load("en_core_web_sm")
        if self.tokenization_method == 'syntok':
            self.tokenizer = Tokenizer(emit_hyphen_or_underscore_sep=True)
            self.stop_words = list(self.nlp(word) for word in self.stop_words)
        elif self.tokenization_method not in ['nltk', 'simple']:
            raise IOError('Please select a valid process method')
        self.stemmed_tokens = {}
        self.a = self.b = self.c = self.d = 0

    def process(self, line: str) -> str:
        if self.tokenization_method == 'syntok':
            # filtered_tokens = [self.stemmer.stem(token.value) for token in self.tokenizer.tokenize(line)
            #                     if not token.value.lower() in self.stop_words]

            # tokens = self.tokenizer.tokenize(line)
            # filtered_tokens = process(self.nlp, tokens, self.stop_words)

            filtered_tokens = []
            # s = time.time()
            # for token in self.tokenizer.tokenize(line):
            #     self.a += time.time() - s
            #     s = time.time()
            #     if not token.value.lower() in self.stop_words:
            #         self.b += time.time() - s
            #         s = time.time()
            #         tok = self.stemmer.stem(token.value)
            #         self.c += time.time() - s
            #         s = time.time()
            #         filtered_tokens.append(tok)
            #         self.d += time.time() - s

            s = time.time()
            for token in self.tokenizer.tokenize(line):
                self.a += time.time() - s
                # check if the word should be stemmed or not
                s = time.time()
                if not token.value.lower() in (self.stop_words or self.stemmed_tokens):
                    self.b += time.time() - s
                    s = time.time()
                    tok = self.stemmer.stem(token.value)
                    self.c += time.time() - s
                    s = time.time()
                    self.stemmed_tokens[token.value] = tok
                    self.d += time.time() - s
                    filtered_tokens.append(tok)
                else:
                    filtered_tokens.append(self.stemmed_tokens[token.value])

        elif self.tokenization_method == 'nltk':
            word_tokens = word_tokenize(line)
            filtered_tokens = [self.stemmer.stem(w) for w in word_tokens if not w.lower() in self.stop_words]
        elif self.tokenization_method == 'simple':
            word_tokens = line.split()
            filtered_tokens = [self.stemmer.stem(w.strip(self.delete_chars_str))
                               for w in word_tokens if not w.lower().strip(self.delete_chars_str) in self.stop_words]
        # print(filtered_tokens)
        return filtered_tokens

    def print_times(self):
        print('a: %f' % self.a)
        print('b: %f' % self.b)
        print('c: %f' % self.c)
        print('d: %f' % self.d)

    def download_nltk_data(self) -> None:
        # create path to data
        if self.file_path not in nltk.data.path:
            nltk.data.path.append(self.file_path)

        # create folder and install necessary packages
        if not os.path.isdir(self.file_path):
            os.mkdir(self.file_path)
            os.system(f'py -m nltk.downloader -d {self.file_path} stopwords')
            os.system(f'py -m nltk.downloader -d {self.file_path} punkt')
