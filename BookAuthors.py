
from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class BookAuthors(MRJob):
    OUTPUT_PROTOCOL = TextValueProtocol # https://pythonhosted.org/mrjob/protocols.html#mrjob.protocol.TextValueProtocol

    def mapper(self, key, line):
        parts = line.split(";")
        try:
            author = parts[2].strip('" ')
            title = parts[1].strip('" ')
            year = int(parts[3].strip('" '))
            yield author, f"{title} ({year})"

        except ValueError:
            pass

    def reducer(self, key, values):
        yield author, f"{key};{';'.join(values)}" # SQL-style joining.

if __name__ == '__main__':
    BookAuthors.run()