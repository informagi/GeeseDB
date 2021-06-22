def get_topics_backgroundlinking(file_name):
    with open(file_name) as topics_file:
        return [topic.strip().split(':') for topic in topics_file.readlines()]