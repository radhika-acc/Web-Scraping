import scrapy

class BestBuySpider(scrapy.Spider):
    name = "bestBuy"

    # Step 1: Set up Start URLs
    start_urls = [
        'https://www.bestbuy.ca/en-ca/category/computers-tablets/20001',
        'https://www.bestbuy.ca/en-ca/category/best-buy-mobile/20006',
        'https://www.bestbuy.ca/en-ca/category/office-supplies-ink/30957',
        'https://www.bestbuy.ca/en-ca/category/tv-home-theatre/20003',
        'https://www.bestbuy.ca/en-ca/category/audio/659699'
    ]

    def parse(self, response):

        # Step 2: Extract the category name from the page.
        category = response.css('h1[class*="title"]::text').get()
        product_containers = response.css('div[class*="productListItem productLine"]')
        
        for post in product_containers:           

            # Step 3: Yield the scraped data.
            yield {
                'category' : category,
                'name_of_product': post.css('div[data-automation="productItemName"]::text').get(),
                'price': float(response.css('div[class*="productPricingContainer"] span::text').get().replace("$","")),
                'saving': post.css('span[class*="productSaving"]::text').get(),
                'rating': float(post.css('span[class*="reviewCountContainer"] meta::attr(content)').get()),
                'reviews' : post.css('span[class*="reviewCountContainer"] span::text').get()
            }

