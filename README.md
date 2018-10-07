## Synopsis

A functional and extremely lightweight application that aggregates Magic: the Gathering data from eBay, normalizes it, and renders it in both text and plot form at the endpoint: https://www.abupower.com. The goal is to allow users to keep tabs on Alpha, Beta, and Unlimited Duals and Power 9 data points.


## Key Python Libraries

- [Flask](http://flask.pocoo.org/) - web framework
- [Jinja2](http://jinja.pocoo.org/) - templating engine
- [Pandas](https://pandas.pydata.org/) - only the best data related lib ever
- [ebaysdk](https://github.com/timotheus/ebaysdk-python) - python wrapper for ebay's api
<!-- - [Linux](http://old-releases.ubuntu.com/releases/16.04.4/) - obv
- [Apache](https://httpd.apache.org/), [PostgreSQL](https://www.postgresql.org/)
- [DigitalOcean](https://www.digitalocean.com/) - server hosting -->

This application is written using [Python 3.6.0](https://www.python.org/downloads/release/python-360/).

## Repository Contents

- `app.py` - The heart of the application and contains the logic that actually renders the various data points into our jinja templates. This is ran when we want to start the application.
- `setup_db.py` - Maps out the desired schema for the database and creates any outlined tables.
- `db_queries.py` - Contains all of the python to postgres connections and database queries, and is what actually populates the data in the application.
- `gen_utils.py` - As the name states, contains general utilities that are used a large number of times in different files.
- `build_dataframes.py` - Used to call every database related function and populate the respective pandas dataframes, representing easy-to-proces containers to pass data around in and manipulate.
- `fetch_active_products.py` - A script to fetch active listings using eBay's `Finding` API (findItemsByKeywords). Ran automatically once a day via a scheduled cron-job.
- `fetch_completed_products.py` - A script to fetch completed listings using eBay's `Finding` API (findCompletedItems). Ran automatically once a day via a scheduled cron-job. Run on a cron-job once a day.
- `populate_active_meta_data.py` -  A script to calculate the stats for all of the active listings in the database before populating a meta_data table. Ran automatically once a day via a scheduled cron-job.
- `populate_completed_meta_data.py` -  A script to calculate the stats for all of the completed listings in the database before populating a meta_data table. Ran automatically once a day via a scheduled cron-job.
- `populate_historical_data.py` -  A script to calculate the stats for all of the completed listings in the database before populating a meta_data table. Ran one time as needed, particularly when adding an entirely new set to the system (i.e. Arabian Nights).

## Pre-filtering & Contributing

[Here](https://github.com/Cooops/newABUPOWER/blob/94995f12bd293820ff013a16d535efe37a4a6942/fetch_completed_products.py#L117) is a link to the current keywords I am pre-filtering. If you have any recommendations, please don't hesitate to reach out. I am always looking to improve the search features (and regex is tough ^^).

## General Thoughts

- The application is certainly not 100% optimized, as this was a learning project first and foremost. The focus was on making it functional before anything else. Overall, I am very satisfied with how it turned out.
- I am not a UX/UI designer, but I tried to make the UI at least bearable. Ping me if you think it is way off.
- I would like to add support for active listings and other sets, but for the time being I am happy with it's minimalistic nature.
- eBay's API is a little wonky...this has been a good lesson that I should be careful about building any application-to-scale on top of or relying on any sort of API.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

Thanks for reading ❤.
