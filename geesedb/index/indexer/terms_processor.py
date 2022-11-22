import os
import types

import nltk.data
import time
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
from syntok.tokenizer import Tokenizer


class TermsProcessor:
    """
    Performs stemming and stop words.
    Uses nltk by default with the process method.
    Inputs the whole document as a string.
    """
    def __init__(self, arguments):
        # stemmer
        if arguments['stemmer'] == 'porter':
            stemmer = PorterStemmer()
            self.stemmer_function = stemmer.stem
        elif arguments['stemmer'] == 'snowball':
            stemmer = SnowballStemmer(arguments['language'])
            self.stemmer_function = stemmer.stem
        elif isinstance(arguments['stemmer'], types.FunctionType):
            self.stemmer_function = arguments['stemmer']
        else:
            raise IOError('Please input a valid stemmer')

        # stop words
        self.file_path = arguments['nltk_path']
        self.delete_chars = arguments['delete_chars']
        self.delete_chars_str = ''.join(self.delete_chars)
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
            raise IOError('Please select a valid option')

        # tokenization
        if arguments['tokenization_method'] == 'syntok':
            t = Tokenizer(emit_hyphen_or_underscore_sep=True)
            self.process = self.process_syntok
            self.tokenizer_function = t.tokenize
        elif arguments['tokenization_method'] == 'nltk':
            self.process = self.process_nltk
            self.tokenizer_function = word_tokenize
        elif isinstance(arguments['tokenization_method'], types.FunctionType):
            self.process = self.process_nltk
            self.tokenizer_function = arguments['tokenization_method']
        else:
            raise IOError('Please select a valid tokenization method')

        # other inits
        self.stemmed_tokens = {}
        self.a = self.b = self.c = 0

    def process_syntok(self, line: str) -> str:
        filtered_tokens = []
        s = time.time()
        for token in self.tokenizer_function(line):
            self.a += time.time() - s
            s = time.time()
            if not token.value.lower() in self.stop_words:
                if token.value not in self.stemmed_tokens:
                    self.b += time.time() - s
                    s = time.time()
                    tok = self.stemmer_function(token.value)
                    self.c += time.time() - s
                    self.stemmed_tokens[token.value] = tok
                    filtered_tokens.append(tok)
                else:
                    filtered_tokens.append(self.stemmed_tokens[token.value])
        # elif self.tokenization_method == 'simple':
        #     word_tokens = line.split()
        #     filtered_tokens = [self.stemmer.stem(w.strip(self.delete_chars_str))
        #                        for w in word_tokens if not w.lower().strip(self.delete_chars_str) in self.stop_words]
        return filtered_tokens

    def process_nltk(self, line: str) -> str:
        filtered_tokens = []
        s = time.time()
        for token in self.tokenizer_function(line):
            self.a += time.time() - s
            s = time.time()
            if not token.lower() in self.stop_words:
                if token not in self.stemmed_tokens:
                    self.b += time.time() - s
                    s = time.time()
                    tok = self.stemmer_function(token)
                    self.c += time.time() - s
                    self.stemmed_tokens[token] = tok
                    filtered_tokens.append(tok)
                else:
                    filtered_tokens.append(self.stemmed_tokens[token])
        return filtered_tokens

    def print_times(self):
        print('    tokenization:                               %f sec' % self.a)
        print('    selecting word for stemming and stop words: %f sec' % self.b)
        print('    stemming:                                   %f sec' % self.c)

    def download_nltk_data(self) -> None:
        # create path to data
        if self.file_path not in nltk.data.path:
            nltk.data.path.append(self.file_path)

        # create folder and install necessary packages
        if not os.path.isdir(self.file_path):
            os.mkdir(self.file_path)
            os.system(f'py -m nltk.downloader -d {self.file_path} stopwords')
            os.system(f'py -m nltk.downloader -d {self.file_path} punkt')
