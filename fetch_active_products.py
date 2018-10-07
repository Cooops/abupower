from ebaysdk.finding import Connection
from ebaysdk.exception import ConnectionError
import time
from gen_utils import get_trace_and_log
import psycopg2
import re
from db_queries import prune_active
from gen_utils import database_connection


active_product_ids = []
active_product_nick = []
active_product_titles = []
active_product_prices = []
active_product_cat_names = []
active_product_cat_ids = []
active_product_img_thumb = []
active_product_img_url = []
active_product_lst_type = []
active_product_con = []
active_product_loc = []
active_product_start = []
active_product_end = []
active_product_watch_count = []
active_product_depth = []
# active_product_avg = []
depthCountStorage = []

class SearchRequest(object):
    def __init__(self, api_key, keyword):
        self.api_key, self.keyword = api_key, keyword
        self.search_body_pages = {
            'keywords': keyword,
            'itemFilter': [
                # {'name': 'LocatedIn', 'value': 'US'},
                # US only sellers -- can also limit by feedback score, business type, top-rated status, charity, etc.
                {'name': 'MinPrice', 'value': '5', 'paramName': 'Currency', 'paramValue': 'USD'},
                {'name': 'MaxPrice', 'value': '99999999', 'paramName': 'Currency', 'paramValue': 'USD'},
                # pre-filter to only actionable items (non-bids, etc.)
                {'name': 'ListingType', 'value': ['FixedPrice', 'StoreInventory', 'AuctionWithBIN']},
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
            api = Connection(siteid='EBAY-US', appid=self.api_key, config_file=None)
            api.execute('findItemsByKeywords', self.search_body_pages)
            self.data = api.response.dict()
            self.pages = int(self.data['paginationOutput']['totalPages'])
            return self.pages
        except ConnectionError as e:
            get_trace_and_log(e)

    def fetch_active_data(self, pages):
        """() -> dict

        Connects to the API,
        Iterates over each page in the previously established range of 1 -> the total number of pages,
        Establish search_body_data parameters,
        Execute a query to find items by their category and takes in predefined parameters search_body_data,
        Return the data in dictionary form,
        Iterates over each item in the returned data dictionary and appends the various data points to their respective lists,
        Prints the values.
        """
        try:
            api = Connection(siteid='EBAY-US', appid=self.api_key, config_file=None)
            search_body_data = {
                'keywords': self.keyword,
                'itemFilter': [  # We also need to REMOVE duplicate listings in the future (?)
                    # {'name': 'LocatedIn', 'value': 'US'},
                    {'name': 'MinPrice', 'value': '5', 'paramName': 'Currency', 'paramValue': 'USD'},
                    {'name': 'MaxPrice', 'value': '99999999', 'paramName': 'Currency', 'paramValue': 'USD'},
                    {'name': 'ListingType', 'value': ['FixedPrice', 'StoreInventory', 'AuctionWithBIN']},
                ],
                'paginationInput':
                    {'entriesPerPage': '100',
                     'pageNumber': f'{page}'},
                # can filter this to multiple different options as well
                'sortOrder': 'PricePlusShippingLowest'}

            api.execute('findItemsByKeywords', search_body_data)
            self.data = api.response.dict()
            time.sleep(1)  # wait a second before continuing (be kind ^^)
        except Exception as e:
            get_trace_and_log(e)

        outliers = [
             re.compile(r"\bce\b", re.I),
             re.compile(r"\bie\b", re.I),
             re.compile(r"\bcollector\b", re.I),
             re.compile(r"\bcollectors\b", re.I),
             re.compile(r"\bcollector's\b", re.I),
             re.compile(r"\bcollector's edition\b", re.I),
             re.compile(r"\binternational\b", re.I),
             re.compile(r"\binternationals\b", re.I),
             re.compile(r"\binternational edition\b", re.I),
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
             re.compile(r"\bcollector''s\b", re.I),
             re.compile(r'\bcollector"s\b', re.I),
             re.compile(r'\b5 x\b', re.I),  # used to ignore things like '5 x Mox's for sale..', which greatly skew the average.
             re.compile(r'\b4 x\b', re.I),
             re.compile(r'\b3 x\b', re.I),
             re.compile(r'\b2 x\b', re.I),
            #  re.compile(r'\b5x\b', re.I),
            #  re.compile(r'\b2x\b', re.I),
            #  re.compile(r'\bx5\b', re.I),
             re.compile(r'\bx4\b', re.I),
            #  re.compile(r'\bx3\b', re.I),
            #  re.compile(r'\bx2\b', re.I),
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
             re.compile(r'\bx-2\b', re.I),
             re.compile(r'\bx-3\b', re.I),
             re.compile(r'\bx-4\b', re.I),
             re.compile(r'\bx 2\b', re.I),
             re.compile(r'\bx 3\b', re.I),
             re.compile(r'\bx 4\b', re.I),
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
            if word.split(' ')[0] in {"Alpha", "Beta", "Unlimited", "Revised"}:
                print(f'Chugging through...{page}/{self.pages} page(s)...')

                depthCount = 0
                for item in self.data['searchResult']['item']:
                    if not any(regex.findall(item['title']) for regex in set(outliers)):  # sets provide more iterating efficiency than lists.
                        # end filter magic => begin appending values to respective arrays
                        try:
                            active_product_con.append(item['condition']['conditionDisplayName'])
                        except Exception as e:
                            active_product_con.append('None')
                        try:
                            active_product_watch_count.append(item['listingInfo']['watchCount'])
                        except Exception as e:
                            active_product_watch_count.append(0)
                        try:
                            active_product_start.append(item['listingInfo']['startTime'])
                        except Exception as e:
                            active_product_start.append(0)
                        active_product_nick.append(word)
                        active_product_titles.append(item['title'])
                        active_product_ids.append(item['itemId'])
                        active_product_prices.append(item['sellingStatus']['currentPrice']['value'])
                        active_product_cat_names.append(item['primaryCategory']['categoryName'])
                        active_product_cat_ids.append(item['primaryCategory']['categoryId'])
                        active_product_img_thumb.append(item['galleryURL'])
                        active_product_img_url.append(item['viewItemURL'])
                        active_product_lst_type.append(item['listingInfo']['listingType'])
                        active_product_loc.append(item['location'])
                        active_product_end.append(item['listingInfo']['endTime'])
                        depthCount += 1

                if self.pages == 1 and page == 1:
                    active_product_depth.extend(depthCount for i in range(depthCount))
                elif self.pages > 1 and page == 1:
                    depthCountStorage.append(depthCount)
                else:
                    depthCountMulti = int(depthCountStorage[-1]) + depthCount
                    active_product_depth.extend(depthCountMulti for i in range(depthCountMulti))
               
                        # print('Title: {}'.format(item['title']))
                        # print('Item ID: {}'.format(item['itemId']))
                        # print('Price: ${}'.format(item['sellingStatus']['currentPrice']['value']))
                        # print('Category Name: {}'.format(item['primaryCategory']['categoryName']))
                        # print('Category ID: {}'.format(item['primaryCategory']['categoryId']))
                        # print('Image Thumbnail: {}'.format(item['galleryURL']))
                        # print('Image URL: {}'.format(item['viewItemURL']))
                        # print('Listing type: {}'.format(item['listingInfo']['listingType']))
                        # print('Product location: {}'.format(item['location']))
                        # print('Condition: {}'.format(item['condition']['conditionDisplayName']))
                        # print('Start time: {}'.format(item['listingInfo']['startTime']))
                        # print('Watchers: {}'.format(item['listingInfo']['watchCount']))
                        # print('Page: {}/{}'.format(int(page), int(self.data['paginationOutput']['totalPages'])))
                        # print()
            else:
                pass
        except KeyError as e:
            get_trace_and_log(e)


class DatabaseConnection(object):
    def __init__(self):
        try:
            self.connection = psycopg2.connect("dbname='EBAYMAGIC' user='postgres' host='localhost' password='magicarb123' port='5432'")
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Successfully connected to database.")
        except Exception as e:
            get_trace_and_log(e)
            print("Cannot connect to database.")

    def insert_active_products(self, zipFile):
        # drop table (drop data, but keep table structure in tact, before piping in new, fresh data) 
        # we would show potentially stale results otherwise
        # begin delete (super raw for now)
        self.cursor.execute("DELETE FROM active_products;")
        # after delete, begin inserting fresh data
        for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o in zipFile:
            try:
                self.cursor.execute("""INSERT INTO active_products(active_product_nick, active_product_titles, active_product_ids, active_product_prices, active_product_cat_names, active_product_cat_ids, active_product_img_thumb, active_product_img_url, active_product_lst_type, active_product_con, active_product_loc, active_product_watch_count, active_product_end, active_product_start, active_product_depth)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o,))  # MAKE SURE to leave the trailing comma (d-->,<--), this will NOT work otherwise.
                print("Unique value inserted...")
            except Exception as e:
                print("Unique value skipped...")
                get_trace_and_log(e)
        print("Successfully piped database.")

if __name__ == '__main__':
    #TODO: could add back in time vaults?
    words = ['Alpha Black Lotus', 'Alpha Mox Sapphire', 'Alpha Mox Jet', 'Alpha Mox Pearl', 'Alpha Mox Ruby', 'Alpha Mox Emerald', 'Alpha Timetwister', 'Alpha Ancestral Recall', 'Alpha Time Walk',
                'Beta Black Lotus MTG', 'Beta Mox Sapphire', 'Beta Mox Jet', 'Beta Mox Pearl', 'Beta Mox Ruby', 'Beta Mox Emerald', 'Beta Timetwister', 'Beta Ancestral Recall', 'Beta Time Walk',
                'Unlimited Black Lotus MTG', 'Unlimited Mox Sapphire', 'Unlimited Mox Jet', 'Unlimited Mox Pearl', 'Unlimited Mox Ruby', 'Unlimited Mox Emerald', 'Unlimited Timetwister', 'Unlimited Ancestral Recall', 'Unlimited Time Walk',
                'Alpha Tundra MTG', 'Alpha Underground Sea MTG', 'Alpha Badlands MTG', 'Alpha Taiga MTG', 'Alpha Savannah MTG', 'Alpha Scrubland MTG', 'Alpha Volcanic Island MTG', 'Alpha Bayou MTG', 'Alpha Plateau MTG', 'Alpha Tropical Island MTG',
                'Beta Tundra MTG', 'Beta Underground Sea MTG', 'Beta Badlands MTG', 'Beta Taiga MTG', 'Beta Savannah MTG', 'Beta Scrubland MTG', 'Beta Volcanic Island MTG', 'Beta Bayou MTG', 'Beta Plateau MTG', 'Beta Tropical Island MTG',
                'Unlimited Tundra MTG', 'Unlimited Underground Sea MTG', 'Unlimited Badlands MTG', 'Unlimited Taiga MTG', 'Unlimited Savannah MTG', 'Unlimited Scrubland MTG', 'Unlimited Volcanic Island MTG', 'Unlimited Bayou MTG', 'Unlimited Plateau MTG', 'Unlimited Tropical Island MTG',
                'Revised Tundra MTG', 'Revised Underground Sea MTG', 'Revised Badlands MTG', 'Revised Taiga MTG', 'Revised Savannah MTG', 'Revised Scrubland MTG', 'Revised Volcanic Island MTG', 'Revised Bayou MTG', 'Revised Plateau MTG', 'Revised Tropical Island MTG']
                # 'Alpha Time Vault MTG', 'Beta Time Vault MTG', 'Unlimited Time Vault MTG']
    # words = ['Beta Badlands MTG']
    # 'Beta Tundra', 'Beta Underground Sea', 'Beta Badlands', 'Beta Taiga', 'Beta Savannah', 'Beta Scrubland', 'Beta Volcanic Island', 'Beta Bayou', 'Beta Plateau', 'Beta Tropical Island'
    # 'Unlimited Tundra', 'Revised Underground Sea', 'Revised Badlands', 'Revised Taiga', 'Revised Savannah', 'Revised Scrubland', 'Revised Volcanic Island', 'Revised Bayou', 'Revised Plateau', 'Revised Tropical Island'
    # 'Alpha Tundra', 'Alpha Underground Sea', 'Alpha Badlands', 'Alpha Taiga', 'Alpha Savannah', 'Alpha Scrubland', 'Alpha Volcanic Island', 'Alpha Bayou', 'Alpha Plateau', 'Alpha Tropical Island'
    # 'CE Tundra', 'CE Underground Sea', 'CE Badlands', 'CE Taiga', 'CE Savannah', 'CE Scrubland', 'CE Volcanic Island', 'CE Bayou', 'CE Plateau', 'CE Tropical Island'
    # 'ICE Tundra', 'ICE Underground Sea', 'ICE Badlands', 'ICE Taiga', 'ICE Savannah', 'ICE Scrubland', 'ICE Volcanic Island', 'ICE Bayou', 'ICE Plateau', 'ICE Tropical Island'
    for word in words:
        print(f'Searching keyword: {word}')
        x = SearchRequest('YOUR-API-KEY-HERE', word)
        pages = x.get_pages() + 1
        for page in range(1, pages):
            x.fetch_active_data(page)
    # print(f"{active_product_titles} {active_product_prices}")
    # print(len(active_product_titles))
    # begin zipping of all arrays into one big-array, just before inserting into the database
    active_products = zip(active_product_nick, active_product_titles, active_product_ids, active_product_prices, active_product_cat_names, active_product_cat_ids, active_product_img_thumb, active_product_img_url, active_product_lst_type, active_product_con, active_product_loc, active_product_watch_count, active_product_end, active_product_start, active_product_depth)
    # begin piping data to the database)
    database_connection = DatabaseConnection()
    database_connection.insert_active_products(active_products)

    # begin pruning of database (removing extreme outliers)
    # cursor = generate_cursor()
    # for value in words:
    #     print(f'Pruning {value}....')
    #     prune_active(value, cursor)
    # print('----------------------------------')
    # print('Succesfully pruned active_products')
    # print('----------------------------------')
