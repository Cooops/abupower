# -*- coding: utf-8 -*-

from flask import Flask, jsonify, request, render_template, make_response, redirect, url_for
from flask_restful import Resource, Api
from jinja2 import Template
import json
import requests

from utils.gen_utils import *
from model.build_dataframes import *
from model.build_models import *


########################
# assign global values #
########################

app = Flask(__name__)
api = Api(app)

legend = 'Index (average)'
priceLegend = 'Price (avg)'
powerLegend = 'P9 Index (average)'
dualsLegend= 'Duals Index (average)'
lengthLegend = 'Average Length (days)'
countLegend = 'Total Sold (listings)'
sumLegend = 'Cumulative Sales (gross)'
depthLegend = 'Active Listings'
spotLegend = 'Spot Price'

headers = {'Content-Type': 'text/html'}

########################################
# begin wholly-rendered rendered pages #
########################################
######################
# begin power routes #
######################
@app.route('/alpha/stats/power/<cardName>')
def renderIndividualAlphaCardPower(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Alpha {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        modCardName = f'Alpha {cardName.capitalize()}'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardTimestampIndividual = list(df_allDataIndividual['timestamp'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/alpha/power/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]

    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual, cardTimestampIndividual=cardTimestampIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/beta/stats/power/<cardName>')
def renderIndividualBetaCardPower(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        if modName[0] == 'black':
            modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()} MTG'
        else:
            modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        modCardName = f'Beta {cardName.capitalize()}'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/beta/power/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/unlimited/stats/power/<cardName>')
def renderIndividualUnlimitedCardPower(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        print(modName)
        if modName[0].lower() == 'black':
            modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
        else:
            modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        modCardName = f'Unlimited {cardName.capitalize()}'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardTimestampIndividual = list(df_allDataIndividual['timestamp'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
   
    url = "https://abupower.com/api/unlimited/power/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual, cardTimestampIndividual=cardTimestampIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/collectors/stats/power/<cardName>')
def renderIndividualCollectorsCardPower(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        print(modName)
        if modName[0].lower() == 'black':
            modCardName = f'Collectors Edition {modName[0].capitalize()} {modName[1].capitalize()} MTG'
        else:
            modCardName = f'Collectors Edition {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        modCardName = f'Collectors Edition {cardName.capitalize()}'
  
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardTimestampIndividual = list(df_allDataIndividual['timestamp'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/collectors/power/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual, cardTimestampIndividual=cardTimestampIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/alpha/stats/duals/<cardName>')
def renderIndividualAlphaCard(cardName):
    cardName = cardName.split('-')
    try:
        modCardName = f'Alpha {cardName[0].capitalize()} {cardName[1].capitalize()} MTG'
    except:
        modCardName = f'Alpha {cardName[0].capitalize()} MTG'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/alpha/duals/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/beta/stats/duals/<cardName>')
def renderIndividualBetaCard(cardName):
    cardName = cardName.split('-')
    try:
        modCardName = f'Beta {cardName[0].capitalize()} {cardName[1].capitalize()} MTG'
    except:
        modCardName = f'Beta {cardName[0].capitalize()} MTG'

    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/beta/duals/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/unlimited/stats/duals/<cardName>')
def renderIndividualUnlimitedCard(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
    else:
        modCardName = f'Unlimited {modName[0].capitalize()} MTG'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/unlimited/duals/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/collectors/stats/duals/<cardName>')
def renderIndividualCollectorsCard(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Collectors Edition {modName[0].capitalize()} {modName[1].capitalize()} MTG'
    else:
        modCardName = f'Collectors Edition {modName[0].capitalize()} MTG'

    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/collectors/duals/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

@app.route('/revised/stats/duals/<cardName>')
def renderIndividualRevisedCard(cardName):
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Revised {modName[0].capitalize()} {modName[1].capitalize()} MTG'
    else:
        modCardName = f'Revised {cardName.capitalize()} MTG'
 
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])

    url = "https://abupower.com/api/revised/duals/table"
    json_data = requests.get(url).json()
    x = json_data['results']
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]

    periodLength = len(cardStatsTimestampIndividual)
    dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
    dateRange = [i.strftime('%b. %d') for i in dateRange]
    periodLengthSpot = len(cardDateIndividual)
    dateRangeSpot = pd.date_range(get_timezones(periodLengthSpot), periods=periodLengthSpot).tolist()
    dateRangeSpot = [i.strftime('%b. %d') for i in dateRangeSpot]
    periodLengthActive = len(cardStatsActiveTimestampIndividual)
    dateRangeActive = pd.date_range(get_timezones(periodLengthActive), periods=periodLengthActive).tolist()
    dateRangeActive = [i.strftime('%b. %d') for i in dateRangeActive]
    
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange, dateRange=dateRange, dateRangeSpot=dateRangeSpot, dateRangeActive=dateRangeActive,
        ), 200, headers)

###################################
# begin individual query endpoint #
###################################
@app.route('/location/')
def location():
    headers = {'Content-Type': 'text/html'}
    query = request.args.get('search')
    if len(query) > 1:
        try:
            set = query.split(' ')[0].lower()
            cardName = query.split(' ', 1)[1].title()  # split only on the first occurence (to avoid filter words with spaces, e.g. Black Lotus)
            if cardName in ('Tundra', 'Underground Sea', 'Badlands', 'Taiga', 'Savannah', 'Scrubland', 'Volcanic Island', 'Bayou', 'Plateau', 'Tropical Island'):
                return redirect(url_for(f'renderIndividual{set.capitalize()}Card', cardName=cardName))
            elif cardName in ('Black Lotus', 'Mox Jet', 'Mox Ruby', 'Mox Emerald', 'Mox Sapphire', 'Mox Pearl', 'Timetwister', 'Time Walk', 'Ancestral Recall'):
                return redirect(url_for(f'renderIndividual{set.capitalize()}CardPower', cardName=cardName))
        except:
            return make_response(render_template('404.html'), 404, headers)  
    else:
        return make_response(render_template('404.html'), 404, headers)  

########################################
# begin email sign-up and other POST's #
########################################
@app.route('/email', methods=['POST'])
def email():
    # TODO: add proper form handling, insert into db, etc etc @ 10/6/2018
    # deal with inserted emails here...insert into db properly
    print('logged email')
    data = request.values
    file = open("emails.txt", "a") 
    file.write(f"{data['name']}: '{data['email']}'\n") 
    file.close() 
    return redirect(url_for('homepage'))

###################################
# begin wholly-rendered endpoints #
###################################
# begin general endpoints
class HomePage(Resource):
    def __init__(self):
        pass
    def get(self):
        # TODO: if we feed in, it's rendered with the page and thus only loaded in `once`. 
        # if we want the data to update/etc, we should use ajax calls to our rest api.
        return make_response(render_template('home.html',
            get_cumulative_power=get_cumulative_power(), get_cumulative_count_power=get_cumulative_count_power(),
            get_cumulative_duals=get_cumulative_duals(), get_cumulative_count_duals=get_cumulative_count_duals()),
            200, headers)

class Active(Resource):
    def __init__(self):
        pass
    def get(self):
        return make_response(render_template('active.html'), 200, headers)  
                  
class About(Resource):
    def __init__(self):
        pass
    def get(self):
        return make_response(render_template('about.html'), 200, headers)

class Footer(Resource):
    def __init__(self):
        pass
    def get(self):
        return make_response(render_template('footer.html'), 200, headers)

class GeneralIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        try:
            return jsonify({'results': {'alpha': calc_index_avg()[0], 'beta': calc_index_avg()[1], 'unlimited': calc_index_avg()[2], 'timestamp': calc_index_avg()[3]}})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class Alpha(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(alphaDataCountPower)) + 1
        minHeight = round(min(alphaDataCountPower)) - 10
        maxHeightLength = round(max(alphaDataLengthPower)) + 2
        minHeightLength = round(min(alphaDataLengthPower)) - 10

        alphaDate = alphaDataLengthTimestampPower
        periodLength = len(alphaDate)
        dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
        dateRange = [i.strftime('%b. %d') for i in dateRange]
        
        alphaDateDuals = alphaDataLengthTimestampDuals
        periodLengthDuals = len(alphaDateDuals)
        dateRangeDuals = pd.date_range(get_timezones(periodLengthDuals), periods=periodLengthDuals).tolist()
        dateRangeDuals = [i.strftime('%b. %d') for i in dateRangeDuals]
        
        return make_response(render_template('alpha.html',
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend, 
            dateRange=dateRange, dateRangeDuals=dateRangeDuals,
            alphaDataAvgDuals=alphaDataAvgDuals, alphaDataAvgTimestampDuals=alphaDataAvgTimestampDuals, 
            alphaDataLengthDuals=alphaDataLengthDuals, alphaDataLengthTimestampDuals=alphaDataLengthTimestampDuals, 
            alphaDataCountDuals=alphaDataCountDuals, alphaDataCountTimestampDuals=alphaDataCountTimestampDuals,
            alphaDataBreakdownNameDuals=alphaDataBreakdownNameDuals, alphaDataBreakdownavg=alphaDataBreakdownAvgDuals,
            alphaDataAllEndDuals=alphaDataAllEndDuals, alphaDataAllNameDuals=alphaDataAllNameDuals, alphaDataAllHrefDuals=alphaDataAllHrefDuals, alphaDataAllPriceDuals=alphaDataAllPriceDuals,
            alphaActiveDataAllStartDuals=alphaActiveDataAllStartDuals, alphaActiveDataAllNameDuals=alphaActiveDataAllNameDuals, alphaActiveDataAllHrefDuals=alphaActiveDataAllHrefDuals, alphaActiveDataAllPriceDuals=alphaActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            alphaDataCumulativePriceDuals=alphaDataCumulativePriceDuals, alphaDataCumulativeTimestampDuals=alphaDataCumulativeTimestampDuals, 
            alphaDataAvgPower=alphaDataAvgPower, alphaDataAvgTimestampPower=alphaDataAvgTimestampPower, 
            alphaDataLengthPower=alphaDataLengthPower, alphaDataLengthTimestampPower=alphaDataLengthTimestampPower, 
            alphaDataCountPower=alphaDataCountPower, alphaDataCountTimestampPower=alphaDataCountTimestampPower,
            alphaDataAllEndPower=alphaDataAllEndPower, alphaDataAllNamePower=alphaDataAllNamePower, alphaDataAllHrefPower=alphaDataAllHrefPower, alphaDataAllPricePower=alphaDataAllPricePower,
            alphaActiveDataAllStartPower=alphaActiveDataAllStartPower, alphaActiveDataAllNamePower=alphaActiveDataAllNamePower, alphaActiveDataAllHrefPower=alphaActiveDataAllHrefPower, alphaActiveDataAllPricePower=alphaActiveDataAllPricePower,
            alphaDataCumulativePricePower=alphaDataCumulativePricePower, alphaDataCumulativeTimestampPower=alphaDataCumulativeTimestampPower,
            get_cumulative=get_data_alpha_cumulative_totals, get_active_depth=get_data_active_index_count_sum), 200, headers)

class AlphaPowerTable(Resource):
    def __init__(self):
        pass
    def get(self):
        alphaPowerTable = clean_to_json(df_alphaStatsPower, 'indexTable')
        alphaPowerTableJSON = json.loads(alphaPowerTable)
        try:
            return jsonify({'results': alphaPowerTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class AlphaPowerIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        alphaPowerIndexAvg = clean_to_json(df_alphaAvgAllPower, 'avg')
        alphaPowerIndexAvgJSON = json.loads(alphaPowerIndexAvg)
        try:
            return jsonify({'results': alphaPowerIndexAvgJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e}) 

class AlphaPowerActive(Resource):
    def __init__(self):
        pass
    def get(self):
        alphaPowerActive = clean_to_json(df_alphaActiveAllPower, 'active')
        alphaPowerActiveJSON = json.loads(alphaPowerActive)
        try:
            return jsonify({'results': alphaPowerActiveJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class AlphaDualsTable(Resource):
    def __init__(self):
        pass
    def get(self):
        alphaDualsTable = clean_to_json(df_alphaStatsDuals, 'indexTable')
        alphaDualsTableJSON = json.loads(alphaDualsTable)
        try:
            return jsonify({'results': alphaDualsTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class AlphaDualsIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Alpha {modName[0].capitalize()} {modName[1].capitalize()} MTG'
            else:
                modCardName = f'Alpha {name.capitalize()} MTG'
            df_alphaIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            alphaDualsIndividualTable = clean_to_json(df_alphaIndividualCardCompletedStats, 'table')
            alphaDualsIndividualTableJSON = json.loads(alphaDualsIndividualTable)
            try:
                return jsonify({'results': alphaDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

class AlphaPowerIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Alpha {modName[0].capitalize()} {modName[1].capitalize()}'
            else:
                modCardName = f'Alpha {name.capitalize()}'
            df_alphaIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            alphaPowerIndividualTable = clean_to_json(df_alphaIndividualCardCompletedStats, 'table')
            alphaPowerIndividualTableJSON = json.loads(alphaPowerIndividualTable)
            try:
                return jsonify({'results': alphaPowerIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

class Beta(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(betaDataCountPower)) + 1
        minHeight = round(min(betaDataCountPower)) - 10
        maxHeightLength = round(max(betaDataLengthPower)) + 2
        minHeightLength = round(min(betaDataLengthPower)) - 10

        betaDate = betaDataLengthTimestampPower
        periodLength = len(betaDate)
        dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
        dateRange = [i.strftime('%b. %d') for i in dateRange]
        
        betaDateDuals = betaDataLengthTimestampDuals
        periodLengthDuals = len(betaDateDuals)
        dateRangeDuals = pd.date_range(get_timezones(periodLengthDuals), periods=periodLengthDuals).tolist()
        dateRangeDuals = [i.strftime('%b. %d') for i in dateRangeDuals]
        
        return make_response(render_template('beta.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            dateRange=dateRange, dateRangeDuals=dateRangeDuals,
            betaDataAvgDuals=betaDataAvgDuals, betaDataAvgTimestampDuals=betaDataAvgTimestampDuals, 
            betaDataLengthDuals=betaDataLengthDuals, betaDataLengthTimestampDuals=betaDataLengthTimestampDuals, 
            betaDataCountDuals=betaDataCountDuals, betaDataCountTimestampDuals=betaDataCountTimestampDuals,
            betaDataBreakdownNameDuals=betaDataBreakdownNameDuals, betaDataBreakdownavg=betaDataBreakdownAvgDuals,
            betaDataAllEndDuals=betaDataAllEndDuals, betaDataAllNameDuals=betaDataAllNameDuals, betaDataAllHrefDuals=betaDataAllHrefDuals, betaDataAllPriceDuals=betaDataAllPriceDuals,
            betaActiveDataAllStartDuals=betaActiveDataAllStartDuals, betaActiveDataAllNameDuals=betaActiveDataAllNameDuals, betaActiveDataAllHrefDuals=betaActiveDataAllHrefDuals, betaActiveDataAllPriceDuals=betaActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            betaDataCumulativePriceDuals=betaDataCumulativePriceDuals, betaDataCumulativeTimestampDuals=betaDataCumulativeTimestampDuals, 
            betaDataAvgPower=betaDataAvgPower, betaDataAvgTimestampPower=betaDataAvgTimestampPower, 
            betaDataLengthPower=betaDataLengthPower, betaDataLengthTimestampPower=betaDataLengthTimestampPower, 
            betaDataCountPower=betaDataCountPower, betaDataCountTimestampPower=betaDataCountTimestampPower,
            betaDataAllEndPower=betaDataAllEndPower, betaDataAllNamePower=betaDataAllNamePower, betaDataAllHrefPower=betaDataAllHrefPower, betaDataAllPricePower=betaDataAllPricePower,
            betaActiveDataAllStartPower=betaActiveDataAllStartPower, betaActiveDataAllNamePower=betaActiveDataAllNamePower, betaActiveDataAllHrefPower=betaActiveDataAllHrefPower, betaActiveDataAllPricePower=betaActiveDataAllPricePower,
            betaDataCumulativePricePower=betaDataCumulativePricePower, betaDataCumulativeTimestampPower=betaDataCumulativeTimestampPower,
            get_cumulative=get_data_beta_cumulative_totals, get_active_depth=get_data_active_index_count_sum), 200, headers)

class BetaPowerTable(Resource):
    def __init__(self):
        pass
    def get(self):
        betaPowerTable = clean_to_json(df_betaStatsPower, 'indexTable')
        betaPowerTableJSON = json.loads(betaPowerTable)
        try:
            return jsonify({'results': betaPowerTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class BetaPowerIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        betaPowerIndexAvg = clean_to_json(df_betaAvgAllPower, 'avg')
        betaPowerIndexAvgJSON = json.loads(betaPowerIndexAvg)
        try:
            return jsonify({'results': betaPowerIndexAvgJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e}) 

class BetaPowerActive(Resource):
    def __init__(self):
        pass
    def get(self):
        betaPowerActive = clean_to_json(df_betaActiveAllPower, 'active')
        betaPowerActiveJSON = json.loads(betaPowerActive)
        try:
            return jsonify({'results': betaPowerActiveJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class BetaDualsTable(Resource):
    def __init__(self):
        pass
    def get(self):
        betaDualsTable = clean_to_json(df_betaStatsDuals, 'indexTable')
        betaDualsTableJSON = json.loads(betaDualsTable)
        try:
            return jsonify({'results': betaDualsTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class BetaDualsIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()} MTG'
            else:
                modCardName = f'Beta {name.capitalize()} MTG'
            df_betaIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            betaDualsIndividualTable = clean_to_json(df_betaIndividualCardCompletedStats, 'table')
            betaDualsIndividualTableJSON = json.loads(betaDualsIndividualTable)
            try:
                return jsonify({'results': betaDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

class BetaPowerIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                if modName[0] == 'black':
                    modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()} MTG'
                else:
                    modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()}'
            else:
                modCardName = f'Beta {name.capitalize()}'
            df_betaIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            betaPowerIndividualTable = clean_to_json(df_betaIndividualCardCompletedStats, 'table')
            betaPowerIndividualTableJSON = json.loads(betaPowerIndividualTable)
            try:
                return jsonify({'results': betaPowerIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

# begin unlimited API endpoints
class Unlimited(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(unlimitedDataCountPower)) + 1
        minHeight = round(min(unlimitedDataCountPower)) - 10
        maxHeightLength = round(max(unlimitedDataLengthPower)) + 2
        minHeightLength = round(min(unlimitedDataLengthPower)) - 10
        headers = {'Content-Type': 'text/html'}
        unlimitedDate = unlimitedDataLengthTimestampPower
        periodLength = len(unlimitedDate)
        dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
        dateRange = [i.strftime('%b. %d') for i in dateRange]
        unlimitedDateDuals = unlimitedDataLengthTimestampDuals
        periodLengthDuals = len(unlimitedDateDuals)
        dateRangeDuals = pd.date_range(get_timezones(periodLengthDuals), periods=periodLengthDuals).tolist()
        dateRangeDuals = [i.strftime('%b. %d') for i in dateRangeDuals]
        return make_response(render_template('unlimited.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            dateRange=dateRange[3::], dateRangeDuals=dateRangeDuals[7::],
            # begin duals
            unlimitedDataAvgDuals=unlimitedDataAvgDuals[7::], unlimitedDataAvgTimestampDuals=unlimitedDataAvgTimestampDuals, 
            unlimitedDataLengthDuals=unlimitedDataLengthDuals[7::], unlimitedDataLengthTimestampDuals=unlimitedDataLengthTimestampDuals, 
            unlimitedDataCountDuals=unlimitedDataCountDuals[7::], unlimitedDataCountTimestampDuals=unlimitedDataCountTimestampDuals,
            unlimitedDataBreakdownNameDuals=unlimitedDataBreakdownNameDuals, unlimitedDataBreakdownavg=unlimitedDataBreakdownAvgDuals,
            unlimitedDataAllEndDuals=unlimitedDataAllEndDuals, unlimitedDataAllNameDuals=unlimitedDataAllNameDuals, unlimitedDataAllHrefDuals=unlimitedDataAllHrefDuals, unlimitedDataAllPriceDuals=unlimitedDataAllPriceDuals,
            unlimitedActiveDataAllStartDuals=unlimitedActiveDataAllStartDuals, unlimitedActiveDataAllNameDuals=unlimitedActiveDataAllNameDuals, unlimitedActiveDataAllHrefDuals=unlimitedActiveDataAllHrefDuals, unlimitedActiveDataAllPriceDuals=unlimitedActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            unlimitedDataCumulativePriceDuals=unlimitedDataCumulativePriceDuals[7::], unlimitedDataCumulativeTimestampDuals=unlimitedDataCumulativeTimestampDuals, 
            # begin power
            unlimitedDataAvgPower=unlimitedDataAvgPower[3::], unlimitedDataAvgTimestampPower=unlimitedDataAvgTimestampPower, 
            unlimitedDataLengthPower=unlimitedDataLengthPower[3::], unlimitedDataLengthTimestampPower=unlimitedDataLengthTimestampPower, 
            unlimitedDataCountPower=unlimitedDataCountPower[3::], unlimitedDataCountTimestampPower=unlimitedDataCountTimestampPower,
            unlimitedDataAllEndPower=unlimitedDataAllEndPower, unlimitedDataAllNamePower=unlimitedDataAllNamePower, unlimitedDataAllHrefPower=unlimitedDataAllHrefPower, unlimitedDataAllPricePower=unlimitedDataAllPricePower,
            unlimitedActiveDataAllStartPower=unlimitedActiveDataAllStartPower, unlimitedActiveDataAllNamePower=unlimitedActiveDataAllNamePower, unlimitedActiveDataAllHrefPower=unlimitedActiveDataAllHrefPower, unlimitedActiveDataAllPricePower=unlimitedActiveDataAllPricePower,
            unlimitedDataCumulativePricePower=unlimitedDataCumulativePricePower[3::], unlimitedDataCumulativeTimestampPower=unlimitedDataCumulativeTimestampPower,
            get_cumulative=get_data_unlimited_cumulative_totals, get_active_depth=get_data_active_index_count_sum), 200, headers)

class UnlimitedPowerTable(Resource):
    def __init__(self):
        pass
    def get(self):
        unlimitedPowerTable = clean_to_json(df_unlimitedStatsPower, 'indexTable')
        unlimitedPowerTableJSON = json.loads(unlimitedPowerTable)
        try:
            return jsonify({'results': unlimitedPowerTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class UnlimitedPowerIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        unlimitedPowerIndexAvg = clean_to_json(df_unlimitedAvgAllPower, 'avg')
        unlimitedPowerIndexAvgJSON = json.loads(unlimitedPowerIndexAvg)
        try:
            return jsonify({'results': unlimitedPowerIndexAvgJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e}) 
                  
class UnlimitedPowerActive(Resource):
    def __init__(self):
        pass
    def get(self):
        unlimitedActivePower = clean_to_json(df_unlimitedActiveAllPower, 'active')
        unlimitedActivePowerJSON = json.loads(unlimitedActivePower)
        try:
            return jsonify({'results': unlimitedActivePowerJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class UnlimitedDualsTable(Resource):
    def __init__(self):
        pass
    def get(self):
        unlimitedDualsTable = clean_to_json(df_unlimitedStatsDuals, 'indexTable')
        unlimitedDualsTableJSON = json.loads(unlimitedDualsTable)
        try:
            return jsonify({'results': unlimitedDualsTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class UnlimitedDualsIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
            else:
                modCardName = f'Unlimited {name.capitalize()} MTG'
            df_unlimitedIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            unlimitedDualsIndividualTable = clean_to_json(df_unlimitedIndividualCardCompletedStats, 'table')
            unlimitedDualsIndividualTableJSON = json.loads(unlimitedDualsIndividualTable)
            try:
                return jsonify({'results': unlimitedDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

class UnlimitedPowerIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                if modName[0] == 'black':
                    modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
                else:
                    modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()}'
            else:
                modCardName = f'Unlimited {name.capitalize()}'
            df_unlimitedIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            unlimitedPowerIndividualTable = clean_to_json(df_unlimitedIndividualCardCompletedStats, 'table')
            unlimitedPowerIndividualTableJSON = json.loads(unlimitedPowerIndividualTable)
            try:
                return jsonify({'results': unlimitedPowerIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})


# begin ce API endpoints
class Ce(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(ceDataCountPower)) + 1
        minHeight = round(min(ceDataCountPower)) - 10
        maxHeightLength = round(max(ceDataLengthPower)) + 2
        minHeightLength = round(min(ceDataLengthPower)) - 10
        headers = {'Content-Type': 'text/html'}
        ceDate = ceDataLengthTimestampPower
        periodLength = len(ceDate)
        dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
        dateRange = [i.strftime('%b. %d') for i in dateRange]
        ceDateDuals = ceDataLengthTimestampDuals
        periodLengthDuals = len(ceDateDuals)
        dateRangeDuals = pd.date_range(get_timezones(periodLengthDuals), periods=periodLengthDuals).tolist()
        dateRangeDuals = [i.strftime('%b. %d') for i in dateRangeDuals]
        return make_response(render_template('ce.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            dateRange=dateRange[3::], dateRangeDuals=dateRangeDuals[7::],
            # begin duals
            ceDataAvgDuals=ceDataAvgDuals[7::], ceDataAvgTimestampDuals=ceDataAvgTimestampDuals, 
            ceDataLengthDuals=ceDataLengthDuals[7::], ceDataLengthTimestampDuals=ceDataLengthTimestampDuals, 
            ceDataCountDuals=ceDataCountDuals[7::], ceDataCountTimestampDuals=ceDataCountTimestampDuals,
            ceDataBreakdownNameDuals=ceDataBreakdownNameDuals, ceDataBreakdownavg=ceDataBreakdownAvgDuals,
            ceDataAllEndDuals=ceDataAllEndDuals, ceDataAllNameDuals=ceDataAllNameDuals, ceDataAllHrefDuals=ceDataAllHrefDuals, ceDataAllPriceDuals=ceDataAllPriceDuals,
            ceActiveDataAllStartDuals=ceActiveDataAllStartDuals, ceActiveDataAllNameDuals=ceActiveDataAllNameDuals, ceActiveDataAllHrefDuals=ceActiveDataAllHrefDuals, ceActiveDataAllPriceDuals=ceActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            ceDataCumulativePriceDuals=ceDataCumulativePriceDuals[7::], ceDataCumulativeTimestampDuals=ceDataCumulativeTimestampDuals, 
            # begin power
            ceDataAvgPower=ceDataAvgPower[3::], ceDataAvgTimestampPower=ceDataAvgTimestampPower, 
            ceDataLengthPower=ceDataLengthPower[3::], ceDataLengthTimestampPower=ceDataLengthTimestampPower, 
            ceDataCountPower=ceDataCountPower[3::], ceDataCountTimestampPower=ceDataCountTimestampPower,
            ceDataAllEndPower=ceDataAllEndPower, ceDataAllNamePower=ceDataAllNamePower, ceDataAllHrefPower=ceDataAllHrefPower, ceDataAllPricePower=ceDataAllPricePower,
            ceActiveDataAllStartPower=ceActiveDataAllStartPower, ceActiveDataAllNamePower=ceActiveDataAllNamePower, ceActiveDataAllHrefPower=ceActiveDataAllHrefPower, ceActiveDataAllPricePower=ceActiveDataAllPricePower,
            ceDataCumulativePricePower=ceDataCumulativePricePower[3::], ceDataCumulativeTimestampPower=ceDataCumulativeTimestampPower,
            get_cumulative=get_data_ce_cumulative_totals, get_active_depth=get_data_active_index_count_sum), 200, headers)

class CePowerTable(Resource):
    def __init__(self):
        pass
    def get(self):
        cePowerTable = clean_to_json(df_ceStatsPower, 'indexTable')
        cePowerTableJSON = json.loads(cePowerTable)
        try:
            return jsonify({'results': cePowerTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class CePowerIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        cePowerIndexAvg = clean_to_json(df_ceAvgAllPower, 'avg')
        cePowerIndexAvgJSON = json.loads(cePowerIndexAvg)
        try:
            return jsonify({'results': cePowerIndexAvgJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e}) 
                  
class CePowerActive(Resource):
    def __init__(self):
        pass
    def get(self):
        ceActivePower = clean_to_json(df_ceActiveAllPower, 'active')
        ceActivePowerJSON = json.loads(ceActivePower)
        try:
            return jsonify({'results': ceActivePowerJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class CeDualsTable(Resource):
    def __init__(self):
        pass
    def get(self):
        ceDualsTable = clean_to_json(df_ceStatsDuals, 'indexTable')
        ceDualsTableJSON = json.loads(ceDualsTable)
        try:
            return jsonify({'results': ceDualsTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class CeDualsIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Ce {modName[0].capitalize()} {modName[1].capitalize()} MTG'
            else:
                modCardName = f'Ce {name.capitalize()} MTG'
            df_ceIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            ceDualsIndividualTable = clean_to_json(df_ceIndividualCardCompletedStats, 'table')
            ceDualsIndividualTableJSON = json.loads(ceDualsIndividualTable)
            try:
                return jsonify({'results': ceDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

class CePowerIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                if modName[0] == 'black':
                    modCardName = f'Ce {modName[0].capitalize()} {modName[1].capitalize()} MTG'
                else:
                    modCardName = f'Ce {modName[0].capitalize()} {modName[1].capitalize()}'
            else:
                modCardName = f'Ce {name.capitalize()}'
            df_ceIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            cePowerIndividualTable = clean_to_json(df_ceIndividualCardCompletedStats, 'table')
            cePowerIndividualTableJSON = json.loads(cePowerIndividualTable)
            try:
                return jsonify({'results': cePowerIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})
                            
# begin revised API endpoints
class Revised(Resource):
    def __init__(self):
        pass
    def get(self):
        revisedDataLengthLegendDuals = 'Average Length (days)'
        revisedDataCountLegendDuals = 'Total Sold (listings)'
        revisedDataCumulativeLegendDuals = 'Cumulative Sales (gross)'
        maxHeight = round(max(revisedDataCountDuals)) + 1
        minHeight = round(min(revisedDataCountDuals)) - 10
        maxHeightLength = round(max(revisedDataLengthDuals)) + 2
        minHeightLength = round(min(revisedDataLengthDuals)) - 10
        headers = {'Content-Type': 'text/html'}
        revisedDate = revisedDataLengthTimestampDuals
        periodLength = len(revisedDate)								  
        dateRange = pd.date_range(get_timezones(periodLength), periods=periodLength).tolist()
        dateRange = [i.strftime('%b. %d') for i in dateRange]				
        return make_response(render_template('revised.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength,
            dateRange=dateRange[2::],
            revisedDataAvgDuals=revisedDataAvgDuals[2::], revisedDataAvgTimestampDuals=revisedDataAvgTimestampDuals, 
            revisedDataLengthDuals=revisedDataLengthDuals[2::], revisedDataLengthTimestampDuals=revisedDataLengthTimestampDuals, revisedDataLengthLegendDuals=revisedDataLengthLegendDuals,
            revisedDataCountDuals=revisedDataCountDuals[2::], revisedDataCountTimestampDuals=revisedDataCountTimestampDuals, revisedDataCountLegendDuals=revisedDataCountLegendDuals,
            revisedDataBreakdownNameDuals=revisedDataBreakdownNameDuals, revisedDataBreakdownavg=revisedDataBreakdownAvgDuals,
            revisedDataAllEndDuals=revisedDataAllEndDuals, revisedDataAllNameDuals=revisedDataAllNameDuals, revisedDataAllHrefDuals=revisedDataAllHrefDuals, revisedDataAllPriceDuals=revisedDataAllPriceDuals,
            revisedActiveDataAllStartDuals=revisedActiveDataAllStartDuals, revisedActiveDataAllNameDuals=revisedActiveDataAllNameDuals, revisedActiveDataAllHrefDuals=revisedActiveDataAllHrefDuals, revisedActiveDataAllPriceDuals=revisedActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            revisedDataCumulativePriceDuals=revisedDataCumulativePriceDuals[2::], revisedDataCumulativeTimestampDuals=revisedDataCumulativeTimestampDuals, revisedDataCumulativeLegendDuals=revisedDataCumulativeLegendDuals,
            get_cumulative=get_data_revised_cumulative_totals, get_active_depth=get_data_active_index_count_sum), 200, headers)


class RevisedDualsTable(Resource):
    def __init__(self):
        pass
    def get(self):
        revisedDualsTable = clean_to_json(df_revisedStatsDuals, 'indexTable')
        revisedDualsTableJSON = json.loads(revisedDualsTable)
        try:
            return jsonify({'results': revisedDualsTableJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})

class RevisedDualsIndexAverage(Resource):
    def __init__(self):
        pass
    def get(self):
        revisedDualsIndexAvg = clean_to_json(df_revisedAvgAllDuals, 'avg')
        revisedDualsIndexAvgJSON = json.loads(revisedDualsIndexAvg)
        try:
            return jsonify({'results': revisedDualsIndexAvgJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e}) 
            
class RevisedDualsActive(Resource):
    def __init__(self):
        pass
    def get(self):
        revisedActiveDuals = clean_to_json(df_revisedActiveAllDuals, 'active')
        revisedActiveDualsJSON = json.loads(revisedActiveDuals)
        try:
            return jsonify({'results': revisedActiveDualsJSON})
        except Exception as e:
            return jsonify({'results': 'failed'},
                        {'error': e})
                        
class RevisedIndividualCardCompletedStats(Resource):
        def __init__(self):
            pass    
        def get(self, name):
            modName = name.replace('%20', ' ').replace('-', ' ').split(' ')
            wordLength = len(modName)
            if wordLength > 1:
                modCardName = f'Revised {modName[0].capitalize()} {modName[1].capitalize()} MTG'
            else:
                modCardName = f'Revised {name.capitalize()} MTG'
            df_revisedIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            revisedDualsIndividualTable = clean_to_json(df_revisedIndividualCardCompletedStats, 'table')
            revisedDualsIndividualTableJSON = json.loads(revisedDualsIndividualTable)
            try:
                return jsonify({'results': revisedDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

#######################
# begin all endpoints #
#######################
# TODO: should these be declared as regular flask routes (although isn't that the same as get-requesting an api endpoint that returns only html?)
# TODO: should this be fed in from a jinja template instead of an api call -> render?
api.add_resource(HomePage, '/')
api.add_resource(Active, '/active')
api.add_resource(About, '/about')
api.add_resource(Footer, '/footer') 
api.add_resource(Alpha, '/alpha')
api.add_resource(Beta, '/beta')
api.add_resource(Unlimited, '/unlimited')
api.add_resource(Revised, '/revised')
api.add_resource(Ce, '/collectors')
# begin alpha api endpoints
api.add_resource(AlphaPowerIndexAverage, '/api/alpha/power/index/avg')
api.add_resource(AlphaPowerActive, '/api/alpha/power/active')
api.add_resource(AlphaDualsTable, '/api/alpha/duals/table')
api.add_resource(AlphaPowerTable, '/api/alpha/power/table')
api.add_resource(AlphaDualsIndividualCardCompletedStats, '/api/alpha/duals/<name>')
api.add_resource(AlphaPowerIndividualCardCompletedStats, '/api/alpha/power/<name>')
# begin beta api endpoints
api.add_resource(BetaPowerIndexAverage, '/api/beta/power/index/avg')
api.add_resource(BetaPowerActive, '/api/beta/power/active')
api.add_resource(BetaDualsTable, '/api/beta/duals/table')
api.add_resource(BetaPowerTable, '/api/beta/power/table')
api.add_resource(BetaDualsIndividualCardCompletedStats, '/api/beta/duals/<name>')
api.add_resource(BetaPowerIndividualCardCompletedStats, '/api/beta/power/<name>')
# begin unlimited api endpoints
api.add_resource(UnlimitedPowerIndexAverage, '/api/unlimited/power/index/avg')
api.add_resource(UnlimitedPowerActive, '/api/unlimited/power/active')
api.add_resource(UnlimitedDualsTable, '/api/unlimited/duals/table')
api.add_resource(UnlimitedPowerTable, '/api/unlimited/power/table')
api.add_resource(UnlimitedDualsIndividualCardCompletedStats, '/api/unlimited/duals/<name>')
api.add_resource(UnlimitedPowerIndividualCardCompletedStats, '/api/unlimited/power/<name>')
# begin ce & ice api endpoints
api.add_resource(CePowerIndexAverage, '/api/collectors/power/index/avg')
api.add_resource(CePowerActive, '/api/collectors/power/active')
api.add_resource(CeDualsTable, '/api/collectors/duals/table')
api.add_resource(CePowerTable, '/api/collectors/power/table')
api.add_resource(CeDualsIndividualCardCompletedStats, '/api/collectors/duals/<name>')
api.add_resource(CePowerIndividualCardCompletedStats, '/api/collectors/power/<name>')
# begin revised api endpoints
api.add_resource(RevisedDualsIndexAverage, '/api/revised/duals/index/avg')
api.add_resource(RevisedDualsActive, '/api/revised/duals/active')
api.add_resource(RevisedDualsTable, '/api/revised/duals/table')
api.add_resource(RevisedIndividualCardCompletedStats, '/api/revised/duals/<name>')
# begin general api endpoints
api.add_resource(GeneralIndexAverage, '/api/general/index')


if __name__ == "__main__":
    TEMPLATES_AUTO_RELOAD = True
    # app.run(host='0.0.0.0')
    app.run(debug=True, port=8050, threaded=True)
