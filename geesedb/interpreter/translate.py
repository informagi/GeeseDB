from .parser import Parser

# This class was used in the paper for translating, all the translating logic is now implemented in Parser
# So this class is a wrapper for that one.
class Translator:

    def __init__(self, database):
        self.parser = Parser(database)

    def translate(self, query):
        return self.parser.parse(query)
