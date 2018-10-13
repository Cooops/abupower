from ebaysdk.finding import Connection
from ebaysdk.exception import ConnectionError
import time
import psycopg2
import re
from db_queries import prune_completed
from gen_utils import database_connection, get_api_key, get_search_words, get_test_search_words, get_trace_and_log

class SearchRequest(object):
    def __init__(self, api_key, keyword):
        self.api_key, self.keyword = api_key, keyword
        # defin which site we wish to connect to and feed in our api-key
        self.api = Connection(siteid='EBAY-US', appid=self.api_key, config_file=None)
        # create a live db cursor
        self.cursor = database_connection()
        # establish lists for appending data to
        self.completed_product_ids = []
        self.completed_product_nick = []
        self.completed_product_titles = []
        self.completed_product_prices = []
        self.completed_product_cat_names = []
        self.completed_product_cat_ids = []
        self.completed_product_img_thumb = []
        self.completed_product_img_url = []
        self.completed_product_lst_type = []
        self.completed_product_con = []
        self.completed_product_loc = []
        self.completed_product_start = []
        self.completed_product_end = []
        self.completed_product_depth = []
        self.depthCountStorage = []
        # outline our search body paramaters
        self.search_body_pages = {
            'keywords': keyword,
            'itemFilter': [
                # US only sellers -- can also limit by feedback score, business type, top-rated status, charity, etc.
                {'name': 'MinPrice', 'value': '59', 'paramName': 'Currency', 'paramValue': 'USD'},
                {'name': 'MaxPrice', 'value': '9999999', 'paramName': 'Currency', 'paramValue': 'USD'},
                # sold items only
                {'name': 'SoldItemsOnly', 'value': 'true'},
            ],
            'paginationInput': {
                'entriesPerPage': '100',
                # always 1, as we want to pull the maximum number of pages given a maximum of 100 results per page
                'pageNumber': '1'
            },
            # can filter this to multiple different options as well (Best Offer, Most Watched, etc.)
            'sortOrder': 'PricePlusShippingLowest'
        }

    def get_pages(self):
        """() -> dict

        Connects to the API,
        Executes a query to find items by their category and takes in predefined parameters search_body_pages,
        Returns the data in dictionary form,
        Returns an integer with the total number of pages.
        """
        try:
            self.api.execute('findCompletedItems', self.search_body_pages)
            self.data = self.api.response.dict()
            self.pages = int(self.data['paginationOutput']['totalPages'])
            return self.pages
        except Exception as e:
            get_trace_and_log(e)

    def fetch_completed_data(self, pages):
        """() -> dict

        Connects to the API,
        Iterates over each page in the previously established range of 1 -> the total number of pages,
        Establishes search_body_data parameters,
        Executes a query to find items by their category and takes in predefined parameters search_body_data,
        Returns the data in dictionary form,
        Iterates over each item in the returned data dictionary and appends the various data points to their respective lists,
        Prints the values.
        """
        try:
            search_body_data = {
                'keywords': self.keyword,
                'itemFilter': [
                    {'name': 'MinPrice', 'value': '5', 'paramName': 'Currency', 'paramValue': 'USD'},
                    {'name': 'MaxPrice', 'value': '99999999', 'paramName': 'Currency', 'paramValue': 'USD'},
                    # sold items only
                    {'name': 'SoldItemsOnly', 'value': 'true'},
                ],
                'paginationInput':
                    {'entriesPerPage': '100',
                     'pageNumber': f'{page}'},
                'sortOrder': 'PricePlusShippingLowest'}

            self.api.execute('findCompletedItems', search_body_data)
            self.data = self.api.response.dict()
            time.sleep(1)  # wait a second before continuing (be kind ^^)
        except Exception as e:
            get_trace_and_log(e)

        outliers = [
            re.compile(r"\bposter\b", re.I),
            re.compile(r"\bproxy\b", re.I),
            re.compile(r"\bmisprint\b", re.I),
            re.compile(r"\bpuzzle\b", re.I),
            re.compile(r"\bplaytest\b", re.I),
            re.compile(r"\berror\b", re.I),
            re.compile(r"\bpromo\b", re.I),
            re.compile(r"\bproxy\b", re.I),
            re.compile(r"\bframed\b", re.I),
            re.compile(r"\breprint\b", re.I),
            re.compile(r"\bbooster\b", re.I),
            re.compile(r"\bpack\b", re.I),
            re.compile(r"\bfactory sealed\b", re.I),
            re.compile(r"\brp\b", re.I),
            re.compile(r"\bheadlamp\b", re.I),
            re.compile(r"\bheadlamps\b", re.I),
            re.compile(r"\bcar\b", re.I),
            re.compile(r"\btruck\b", re.I),
            re.compile(r"\bheadlights\b", re.I),
            re.compile(r"\brepack\b", re.I),
            re.compile(r"\brepacks\b", re.I),
            re.compile(r"\brubber\b", re.I),
            re.compile(r"\bseat\b", re.I),
            re.compile(r"\bbox\b", re.I),
            re.compile(r'\bsticker\b', re.I),
            re.compile(r'\bstickers\b', re.I),
            re.compile(r'\b5 x\b', re.I),  # used to ignore things like '5 x Mox's for sale..', which greatly skew the average.
            re.compile(r'\b4 x\b', re.I),
            re.compile(r'\b3 x\b', re.I),
            re.compile(r'\b2 x\b', re.I),
            re.compile(r'\b5x\b', re.I),
            re.compile(r'\b4x\b', re.I),
            re.compile(r'\b3x\b', re.I),
            re.compile(r'\b2x\b', re.I),
            re.compile(r'\bx5\b', re.I),
            re.compile(r'\bx4\b', re.I),
            re.compile(r'\bx3\b', re.I),
            re.compile(r'\bx2\b', re.I),
            re.compile(r'\bx-2\b', re.I),
            re.compile(r'\bx-3\b', re.I),
            re.compile(r'\bx-4\b', re.I),
            re.compile(r'\bx-5\b', re.I),
            re.compile(r'\bx 2\b', re.I),
            re.compile(r'\bx 3\b', re.I),
            re.compile(r'\bx 4\b', re.I),
            re.compile(r'\bx 5\b', re.I),
            re.compile(r'\bcustom\b', re.I),
            re.compile(r'\bpractice\b', re.I),
            re.compile(r'\btime spiral\b', re.I),
            re.compile(r'\blions\b', re.I),
            re.compile(r'\bstory\b', re.I),
            re.compile(r'\bmullet\b', re.I),
            re.compile(r'\bplayset\b', re.I),
            re.compile(r'\bbb\b', re.I),
            re.compile(r'\bblack border\b', re.I),
            re.compile(r'\bartist proof\b', re.I),
            re.compile(r'\bgerman\b', re.I),
            re.compile(r'\bitalian\b', re.I),
            re.compile(r'\bfrench\b', re.I),
            re.compile(r'\blot\b', re.I),
            re.compile(r'\bsealed\b', re.I),
            re.compile(r'\bartist\b', re.I),
            re.compile(r'\bproof\b', re.I),
            re.compile(r'\bcollection\b', re.I),
            re.compile(r'\bfbb\b', re.I),
            #  re.compile(r'\b2\b', re.I),
            #  re.compile(r'\b3\b', re.I),
            #  re.compile(r'\b4\b', re.I),
            #  re.compile(r'\b5\b', re.I),
            #  re.compile(r'\b6\b', re.I),
            re.compile(r'\bcomplete set\b', re.I),
            re.compile(r'\bplayset\b', re.I),
            re.compile(r'\bplay-set\b', re.I),
            re.compile(r'\bset\b', re.I),
            re.compile(r'\b(Partial)\b', re.I),
            re.compile(r'\bpartial\b', re.I),
            re.compile(r'\binfect\b', re.I),
        ]
        try:
            # begin filtering magic :D
            if word.split(' ')[0] not in {"Collector's", "International"}:
                outliers.extend((
                    re.compile(r"\bce\b", re.I),
                    re.compile(r"\bie\b", re.I),
                    re.compile(r"\bcollector\b", re.I),
                    re.compile(r"\bcollectors\b", re.I),
                    re.compile(r"\bcollector's\b", re.I),
                    re.compile(r"\bcollector's edition\b", re.I),
                    re.compile(r"\binternational\b", re.I),
                    re.compile(r"\binternationals\b", re.I),
                    re.compile(r"\binternational edition\b", re.I),
                    re.compile(r"\bcollector''s\b", re.I),
                    re.compile(r'\bcollector"s\b', re.I),
                ))
            else:
                pass
                # print(f'Searching keyword: {word}', end="")
                print(f'Searching keyword: {word}')
                print(f'Chugging through...{page}/{self.pages} page(s)...')
                print()
                depthCount = 0
                for item in self.data['searchResult']['item']:
                    if not any(regex.findall(item['title']) for regex in set(outliers)):  # sets provide more iterating efficiency than lists.
                        # end filter magic => begin appending values to respective arrays
                        try:
                            self.completed_product_img_thumb.append(item['galleryURL'])
                        except Exception as e:
                            self.completed_product_img_thumb.append('No picture')
                        self.completed_product_nick.append(word)
                        self.completed_product_titles.append(item['title'])
                        self.completed_product_ids.append(item['itemId'])
                        # completed_product_prices.append(item['sellingStatus']['currentPrice']['value'])
                        self.completed_product_prices.append(item['sellingStatus']['convertedCurrentPrice']['value'])  # take the convertedCurrentPrice instead @ 10/10/2018
                        self.completed_product_cat_names.append(item['primaryCategory']['categoryName'])
                        self.completed_product_cat_ids.append(item['primaryCategory']['categoryId'])
                        self.completed_product_img_url.append(item['viewItemURL'])
                        self.completed_product_lst_type.append(item['listingInfo']['listingType'])
                        self.completed_product_con.append(item['condition']['conditionDisplayName'])
                        self.completed_product_loc.append(item['location'])
                        self.completed_product_start.append(item['listingInfo']['startTime'])
                        self.completed_product_end.append(item['listingInfo']['endTime'])  
                        depthCount += 1
                # if the page is 1 and the max number of pages is 1 then extend the depth to fill up the list, 
                # otherwise proceed forward
                if self.pages == 1 and page == 1:
                    self.completed_product_depth.extend(depthCount for i in range(depthCount))
                elif self.pages > 1 and page == 1:
                    self.depthCountStorage.append(depthCount)
                else:
                    depthCountMulti = int(self.depthCountStorage[-1]) + depthCount
                    self.completed_product_depth.extend(depthCountMulti for i in range(depthCountMulti))
        except KeyError as e:
            get_trace_and_log(e)

    def zip_items(self):
        """(lists) -> (zip)

        Inherits a series of lists and wraps it up into a comprehensive zip."""
        #"begin zipping of all arrays into one big-array, just before inserting into the database
        self.completed_products = zip(self.completed_product_nick, self.completed_product_titles, self.completed_product_ids, self.completed_product_prices, self.completed_product_cat_names, self.completed_product_cat_ids, self.completed_product_img_thumb, self.completed_product_img_url, self.completed_product_lst_type, self.completed_product_con, self.completed_product_loc, self.completed_product_start, self.completed_product_end, self.completed_product_depth)
        return self.completed_products

    def insert_completed_products(self, count):
        """(db cursor, array, count) -> ()

        Takes in a database connection (cursor) and an array of data and inserts it into the respective database"""
        for a, b, c, d, e, f, g, h, i, j, k, l, m, n in self.completed_products:
            try:
                self.cursor.execute("""INSERT INTO completed_products(completed_product_nick, completed_product_titles, completed_product_ids, completed_product_prices, completed_product_cat_names, completed_product_cat_ids, completed_product_img_thumb, completed_product_img_url, completed_product_lst_type, completed_product_con, completed_product_loc, completed_product_start, completed_product_end, completed_product_depth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (a, b, c, d, e, f, g, h, i, j, k, l, m, n, ))  # MAKE SURE to leave the trailing comma (d-->,<--), this will NOT work otherwise.
                print("Unique value inserted...")
            except Exception as e:
                print("Unique value skipped...")
                # get_trace_and_log(e)
        print()
        print("Successfully piped database.")


if __name__ == '__main__':
     # pull in our api key, the list of words to iterate over, and begin zipping lists before piping the db
    api_key = get_api_key()
    # comment out the above variable and use the one below when testing (includes 3 very common values)
    words = get_test_search_words()
    # words = ["Collector's Edition Black Lotus MTG", "International Edition Mox Ruby MTG", "Beta Black Lotus MTG"]
    count = 0
    for word in words:
        # print(word)
        count += 1
        x = SearchRequest(api_key, word)
        pages = x.get_pages() + 1
        for page in range(1, pages):
            x.fetch_completed_data(page)
        x.zip_items()
        x.insert_completed_products(count)
