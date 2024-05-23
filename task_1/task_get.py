from task_models import Quotes, Authors
import task_connect


def search_by_name(args):
    name = args[0]
    author_obj = Authors.objects(fullname=name)
    author_id = [author.id for author in author_obj][0]

    quotes = Quotes.objects(author=author_id)
    print(f"author: {name}\n _______")
    for q in quotes:
        tags = [tag.name for tag in q.tags]
        quote = q.quote
        print(f"tags: {tags}\n quote: {quote}")


def search_by_tag(args):
    tag = args[0]
    tag = tag.strip().lower()
    quotes = Quotes.objects(tags__name=tag)
    print(f"tag: {tag}\n _______")
    for q in quotes:
        author = q.author.fullname
        quote = q.quote
        tags = [tag.name for tag in q.tags]
        print(f"author: {author}\n quote: {quote}\n tags: {tags}")


def search_by_tags(args):
    quotes = Quotes.objects(tags__name__in=args)
    print(f"tags: {args}\n _______")
    for q in quotes:
        author = q.author.fullname
        quote = q.quote
        tags = [tag.name for tag in q.tags]
        print(f"author: {author}\n quote: {quote}\n tags: {tags}")
