from task_models import Authors, Tags, Quotes
import task_connect
import json


# Function to load JSON data
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


# Function to insert authors into MongoDB
def insert_authors(authors_data):
    authors_dict = {}
    for author_data in authors_data:
        author = Authors(**author_data)
        author.save()
        authors_dict[author.fullname] = author
    return authors_dict


# Function to insert quotes into MongoDB
def insert_quotes(quotes_data, authors_dict):
    for quote_data in quotes_data:
        tags = [Tags(name=tag) for tag in quote_data['tags']]
        author = authors_dict.get(quote_data['author'])
        quote = Quotes(tags=tags, author=author, quote=quote_data['quote'])
        quote.save()


# Load JSON data
authors_data = load_json_data('task_authors.json')
quotes_data = load_json_data('task_qoutes.json')

if __name__ == "__main__":
    # Insert data into MongoDB
    authors_dict = insert_authors(authors_data)
    insert_quotes(quotes_data, authors_dict)
