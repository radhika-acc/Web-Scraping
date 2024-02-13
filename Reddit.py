import scrapy

class RedditSpider(scrapy.Spider):
    name = "reddit"
    start_urls = ['https://old.reddit.com/r/datascience/',
                  'https://old.reddit.com/r/artificial/',
                  'https://old.reddit.com/r/MachineLearning/',
                  'https://old.reddit.com/r/explainlikeimfive/']

    def parse(self, response):
        for post in response.css('div[class*="thing"]'):
            yield {
                'title': post.css('p.title a::text').get(),
                'poster': post.css('div[class*="thing"]::attr(data-author)').get(),
                'time': post.css('p.tagline time::attr(title)').get(),
                'subreddit': post.css('div[class*="thing"]::attr(data-subreddit-prefixed)').get(),
                'initial_post': post.css('div[class*="thing"]::attr(data-url)').getall(),
            }
