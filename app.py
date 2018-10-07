from flask import Flask, jsonify, request, render_template, make_response, redirect, url_for
from flask_restful import Resource, Api
from build_dataframes import *
import json
from jinja2 import Template
import requests
from gen_utils import fetch_data

##############################
# assign global Flask values #
##############################
# begin app instantiation
app = Flask(__name__)
api = Api(app)

##################################
# begin global memo dictionaries #
##################################
# begin memos
avgMemo = {}
lenMemo = {}
countMemo = {}
sumMemo = {}
spreadMemo = {}
avgActiveMemo = {}
depthMemo = {}
depth = []

def get_percent_change_last_sold(name, value, boolCheck):
    if boolCheck == True:
        newValue = get_data_single_product_avg(name)
        calc = ((float(value.replace(',','')) - float(newValue[0])) / float(newValue[0])) * 100
        if calc == 0:
            # TODO: there may be a better/easier way to do this -- ajax perhaps? @ 9/8/2018
            return f"±{calc:.1f}%", 'color: hsl(208, 9%, 38%);', newValue
        elif calc < 0:
            return f"{calc:.1f}%", 'color: rgb(179,63,40);', newValue
        elif calc > 0:
            return f"+{calc:.1f}%", 'color: hsl(110, 51%, 41%);', newValue
    elif boolCheck == False:
        newValue = name
        calc = ((float(value) - float(newValue)) / float(newValue)) * 100
        if calc == 0:
            return f"±{calc:.1f}%", 'color: hsl(208, 9%, 38%);', name
        elif calc < 0:
            return f"{calc:.1f}%", 'color: rgb(179,63,40);', name
        elif calc > 0:
            return f"+{calc:.1f}%", 'color: hsl(110, 51%, 41%);', name

def calc_premiums(firstArray, secondArray):
    calc = [((float(firstAvg) - float(secondAvg))/float(secondAvg))*100 for firstAvg, secondAvg in zip(firstArray, secondArray)]
    calcSum = sum(calc)
    calcLen = len(calc)
    finalCalc = calcSum/calcLen
    if finalCalc == 0:
        return f"(±{finalCalc}%)", 'color: hsl(208, 9%, 38%) !important;'
    elif finalCalc < 1:
        return f"({finalCalc:.2f}%)", 'color: rgb(179,63,40) !important;'
    elif finalCalc > 1:
        return f"(+{finalCalc:.2f}%)", 'color: rgb(143,198,132) !important;'

def get_premiums():    
    # TODO: add support for ce/ice, etc ? @ 9/20/2018
    # begin get individual averages
    alphaAverage = get_data_alpha_breakdown()
    betaAverage = get_data_beta_breakdown()
    unlimitedAverage = get_data_unlimited_breakdown()
    # begin calculations to deduce the average premium between the respective sets
    alphaToBetaPremium = calc_premiums(list(alphaAverage['completed_product_avg'].values), betaAverage['completed_product_avg'].values)
    alphaToUnlimitedPremium  = calc_premiums(alphaAverage['completed_product_avg'].values, unlimitedAverage['completed_product_avg'].values)
    betaToAlphaPremium  = calc_premiums(betaAverage['completed_product_avg'].values, alphaAverage['completed_product_avg'].values)
    betaToUnlimitedPremium  = calc_premiums(betaAverage['completed_product_avg'].values, unlimitedAverage['completed_product_avg'].values)
    unlimitedToAlphaPremium  = calc_premiums(unlimitedAverage['completed_product_avg'].values, alphaAverage['completed_product_avg'].values)
    unlimitedToBetaPremium  = calc_premiums(unlimitedAverage['completed_product_avg'].values, betaAverage['completed_product_avg'].values)
    # assign data values before returning dict
    return {'alphaToBeta': alphaToBetaPremium, 
            'alphaToUnlimited': alphaToUnlimitedPremium, 
            'betaToAlpha': betaToAlphaPremium, 
            'betaToUnlimited': betaToUnlimitedPremium,
            'unlimitedToAlpha': unlimitedToAlphaPremium,
            'unlimitedToBeta': unlimitedToBetaPremium,}

#TODO: can i move this into the function in an easy manner -- just pass as an argument perhaps? @ 9/24/2018
# definitely do a write up on the speed difference between sending infinite queries vs 'caching' them w/
# memoization, as it increases the speed _tenfold_
def memoize(dataframe, check, subArray):
    if check == 'avg':
        try:
            for name in dataframe.values:
                if name not in avgMemo:
                    avgMemo[name] = f'${float(get_data_single_product_avg(name)[0]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'length':
        try:
            for name in dataframe.values:
                if name not in lenMemo:
                    lenMemo[name] = f'{float(get_data_single_product_avg_length_90(name)[0]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'count':
        try:
            for name in dataframe.values:
                if name not in countMemo:
                    countMemo[name] = f'{float(get_data_single_product_count_90(name)[0]):.0f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'avgActive':
        try:
            for name in dataframe.values:
                if name not in avgActiveMemo:
                    avgActiveMemo[name] = f'${float(get_data_single_product_avg(name)[0]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'avgSecond':
        try:
            for name in dataframe.values:
                if name not in avgMemo:
                    avgMemo[name] = f'${float(get_data_single_product_avg(name)[1]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'countSecond':
        try:
            for name in dataframe.values:
                if name not in countMemo:
                    countMemo[name] = f'{float(get_data_single_product_count_90(name)[1]):.0f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'lengthSecond':
        try:
            for name in dataframe.values:
                if name not in lenMemo:
                    lenMemo[name] = f'{float(get_data_single_product_avg_length_90(name)[1]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'sumSecond':
        try:
            for name in dataframe.values:
                if name not in sumMemo:
                    sumMemo[name] = f'{float(get_data_single_product_sum_90(name)[1]):.2f}'
        except Exception as e:
            get_trace_and_log(e)
    elif check == 'depthSecond':
        try:
            for name in dataframe.values:
                if name not in depthMemo:
                    depth.append(get_data_single_product_depth_last_2(name)[0][0])
                    depthMemo[name] = f'{float(get_data_single_product_depth_last_2(name)[1][0]):.2f}'
                    # depthMemo[name] = f'{float(get_data_single_product_depth_last_2(name)[1][0]):.2f}'
        except Exception as e:
            get_trace_and_log(e)


def clean_to_json(dataframe, apiValue):
    if apiValue == 'indexTable':
        jsonList = []
        nick = dataframe['completed_product_nick'].values
        price = dataframe['completed_product_avg'].values
        count = dataframe['completed_product_depth'].values
        length = dataframe['completed_product_avg_length'].values
        cumSum = dataframe['completed_product_sum'].values
        # depth = []
        # depth = dataframe['completed_product_depth'].values
        avgYesterday = []
        lengthYesterday = []
        countYesterday = []
        sumYesterday = []
        depthYesterday = []
        # begin memoization for sending stat-based queries (different table, different number of values 
        # (i.e, 10x Black Lotus _listings_, 1x Black Lotus _average_))
        memoize(dataframe['completed_product_nick'], 'avgSecond', [])
        for value in nick:
            if value in avgMemo:
                avgYesterday.append(avgMemo[value])
        # begin memoization for avg length
        memoize(dataframe['completed_product_nick'], 'lengthSecond', [])
        for value in nick:
            if value in lenMemo:
                lengthYesterday.append(lenMemo[value])
        # begin memoization for total sold
        memoize(dataframe['completed_product_nick'], 'countSecond', [])
        for value in nick:
            if value in countMemo:
                countYesterday.append(countMemo[value])
        # begin memoization for cumulative sum (total gross)
        memoize(dataframe['completed_product_nick'], 'sumSecond', [])
        for value in nick:
            if value in sumMemo:
                sumYesterday.append(sumMemo[value])
        # begin memoization for cumulative sum (total gross)
        memoize(dataframe['completed_product_nick'], 'depthSecond', [])
        for value in nick:
            if value in depthMemo:
                # depth.append(depthMemo[value][0][0])
                depthYesterday.append(depthMemo[value])
        # begin priceChange calculation
        priceChange = [(float(x)-float(y.split('$')[1].replace(',','')))/float(y.split('$')[1].replace(',',''))*100 for x,y in zip(price,avgYesterday)]
        # begin priceChange calculation
        countChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(count,countYesterday)]
        # begin priceChange calculation
        lengthChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(length,lengthYesterday)]
        # begin sumChange calculation
        sumChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(cumSum,sumYesterday)]
        # begin depthChange calculation
        depthChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(depth,depthYesterday)]
        # print(depth)
        # begin zipping values before serializing to JSON (aka `dumping`)
        # print(nick, price)
        # if len(priceChange) < 0:
            # priceChange.extend('0')
        for a, b, c, d, e, f, g, h, i, j in zip(nick, price, priceChange, length, lengthChange, count, countChange, cumSum, sumChange, depthChange):
            # json can't serialize dates, which is why we convert the `end` variable into a string
            jsonList.append(
                    {'nick': a,
                    'price': f'${float(b):.2f}',
                    'priceChange': f'{float(c):.2f}%',
                    'length': f'{float(d):.2f}',
                    'lengthChange': f'{float(e):.2f}%',
                    'count': str(f),
                    'countChange': f'{float(g):.2f}%',
                    'cumSum': f'${float(h):,}',
                    'sumChange': f'{float(i):.2f}%',
                    'depthChange': f'{float(j):.2f}%',
                    }
                )
        return json.dumps(jsonList)
    elif apiValue == 'table':
        jsonList = []
        nick = dataframe['completed_product_nick'].values
        title = dataframe['completed_product_titles'].values
        price = dataframe['completed_product_prices'].values
        # end = dataframe['completed_product_end']
        end = dataframe['enddate']
        url = dataframe['completed_product_img_url'].values
        thumb = dataframe['completed_product_img_thumb'].values
        type = dataframe['completed_product_lst_type'].values
        loc = dataframe['completed_product_loc'].values
        regLength = dataframe['length']
        # depth = dataframe['completed_product_depth'].values
        avg = []
        length = []
        count = []
        # begin memoization for sending stat-based queries (different table, different number of values 
        # (i.e, 10x Black Lotus _listings_, 1x Black Lotus _average_))
        memoize(dataframe['completed_product_nick'], 'avg', [])
        for value in nick:
            if value in avgMemo:
                avg.append(avgMemo[value])
        # begin memoization for avg length
        memoize(dataframe['completed_product_nick'], 'length', [])
        for value in nick:
            if value in lenMemo:
                length.append(lenMemo[value])
        # begin memoization for total sold
        memoize(dataframe['completed_product_nick'], 'count', [])
        for value in nick:
            if value in countMemo:
                count.append(countMemo[value])
        # begin spread calculation
        spread = [(float(x)-float(y.split('$')[1].replace(',','')))/float(y.split('$')[1].replace(',',''))*100 for x,y in zip(price,avg)]
        # # begin priceChange calculation
        # countChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(count,countYesterday)]
        # begin priceChange calculation
        lengthChange = [(float(x)-float(y))/float(y)*100 for x,y in zip(regLength,length)]
        # begin zipping values before serializing to JSON (aka `dumping`)
        for a, b, c, d, e, f, g, h, i, j, k, l, m, n in zip(nick, title, price, avg, end, url, thumb, type, loc, length, count, spread, regLength, lengthChange):
            # json can't serialize dates, which is why we convert the `end` variable into a string
            jsonList.append(
                    {'nick': a,
                    'title': b,
                    'price': c,
                    'avg': d,
                    'end': str(e), 
                    'url': f,
                    'thumb': g,
                    'type': h,
                    'loc': i,
                    'length': j,
                    'count': k,
                    'spread': l,
                    'regLength': str(m),
                    'lengthChange': n,
                    }
                )
        return json.dumps(jsonList)
    elif apiValue == 'avg':
        jsonList = []
        indexAvg = dataframe['completed_product_index_avg'].values
        indexEnd = dataframe['timestamp']
        for a, b in zip(indexAvg, indexEnd):
            # json can't serialize dates, which is why we convert the `end` variable into a string
            jsonList.append(
                    {'indexAvg': a,
                    'indexEnd': str(b)
                    }
                )
        return json.dumps(jsonList)
    elif apiValue == 'active':
        jsonList = []
        activeStart = list(dataframe['active_product_start'])
        activeName = list(dataframe['active_product_nick'])
        activeTitle = list(dataframe['active_product_titles'])
        activeHref = list(dataframe['active_product_img_url'])
        activePrice = list(dataframe['active_product_prices'])
        activeThumb = list(dataframe['active_product_img_thumb'])
        activeDepth = list(dataframe['active_product_depth'])
        avg = []
        length = []
        count = []
        # begin memoization for sending stat-based queries (`avg`, in this case) 
        memoize(dataframe['active_product_nick'], 'avgActive', [])
        for value in activeName:
            if value in avgActiveMemo:
                avg.append(avgActiveMemo[value])
        # begin memoization for avg length
        memoize(dataframe['active_product_nick'], 'length', [])
        for value in activeName:
            if value in lenMemo:
                length.append(lenMemo[value])
        # begin memoization for total sold
        memoize(dataframe['active_product_nick'], 'count', [])
        for value in activeName:
            if value in countMemo:
                count.append(countMemo[value])
        # begin spread calculation
        spread = [(float(x.split('$')[1].replace(',',''))-float(y.split('$')[1].replace(',','')))/float(y.split('$')[1].replace(',',''))*100 for x,y in zip(activePrice,avg)]
        for a, b, c, d, e, f, g, h, i, j, k in zip(activeStart, activeName, activeHref, activePrice, avg, activeThumb, activeTitle, spread, activeDepth, count, length):
            # json can't serialize dates, which is why we convert the `end` variable into a string
            jsonList.append(
                    {'activeStart': str(a),
                    'activeName': b,
                    'activeHref': c,
                    'activePrice': d,
                    'avg': e,
                    'activeThumb': f,
                    'activeTitle': g,
                    'spread': h,
                    'activeDepth': int(i),
                    'count': j,
                    'length': k,
                    }
                )
        return json.dumps(jsonList)

def calc_index_avg():
    alphaIndexDataAvg = list(df_alphaAvgAllPower['completed_product_index_avg'])
    betaIndexDataAvg = list(df_betaAvgAllPower['completed_product_index_avg'])
    unlimitedIndexDataAvg = list(df_unlimitedAvgAllPower['completed_product_index_avg'])
    metaIndexDominanceTimestamp = list(df_unlimitedAvgAllPower['timestamp'].values)

    metaIndexSubtotal = [a + b + c for a, b, c in zip(alphaIndexDataAvg, betaIndexDataAvg, unlimitedIndexDataAvg)]
    alphaIndexDominanceLevel = [(a / b) * 100 for a, b in zip(alphaIndexDataAvg, metaIndexSubtotal)]
    betaIndexDominanceLevel = [(a / b) * 100 for a, b in zip(betaIndexDataAvg, metaIndexSubtotal)]
    unlimitedIndexDominanceLevel = [(a / b) * 100 for a, b in zip(unlimitedIndexDataAvg, metaIndexSubtotal)]
    metaIndexDominanceTimestamp = [str(each).split(',')[0] for each in metaIndexDominanceTimestamp]

    return alphaIndexDominanceLevel, betaIndexDominanceLevel, unlimitedIndexDominanceLevel, metaIndexDominanceTimestamp

# begin global legend variables
legend = 'Index (average)'
powerLegend = 'P9 Index (average)'
dualsLegend= 'Duals Index (average)'
lengthLegend = 'Average Length (days)'
countLegend = 'Total Sold (listings)'
sumLegend = 'Cumulative Sales (gross)'

############################################
# begin wholly-rendered pre-rendered pages #
############################################

# begin power routes
# begin alpha routes
@app.route('/alpha/stats/power/<cardName>')
def renderIndividualAlphaCardPower(cardName):
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Alpha {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        modCardName = f'Alpha {cardName.capitalize()}'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/alpha/power/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)
# begin beta routes
@app.route('/beta/stats/power/<cardName>')
def renderIndividualBetaCardPower(cardName):
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        if modName[0] == 'black':
            modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()} MTG'
        else:
            modCardName = f'Beta {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        # print(cardName)
        modCardName = f'Beta {cardName.capitalize()}'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/beta/power/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)
# begin unlimited routes
@app.route('/unlimited/stats/power/<cardName>')
def renderIndividualUnlimitedCardPower(cardName):
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        print(modName)
        if modName[0].lower() == 'black':
            modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
        else:
            modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()}'
    else:
        # print(cardName)
        modCardName = f'Unlimited {cardName.capitalize()}'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/unlimited/power/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)

# begin duals routes
# begin alpha routes
@app.route('/alpha/stats/duals/<cardName>')
def renderIndividualAlphaCard(cardName):
    # print(cardName)
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    cardName = cardName.split('-')
    try:
        modCardName = f'Alpha {cardName[0].capitalize()} {cardName[1].capitalize()} MTG'
    except:
        modCardName = f'Alpha {cardName[0].capitalize()} MTG'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/alpha/duals/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)

# begin beta routes
@app.route('/beta/stats/duals/<cardName>')
def renderIndividualBetaCard(cardName):
    # print(cardName)
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    cardName = cardName.split('-')
    try:
        modCardName = f'Beta {cardName[0].capitalize()} {cardName[1].capitalize()} MTG'
    except:
        modCardName = f'Beta {cardName[0].capitalize()} MTG'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/beta/duals/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)

# begin unlimited routes
@app.route('/unlimited/stats/duals/<cardName>')
def renderIndividualUnlimitedCard(cardName):
    # print(cardName)
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
    else:
        modCardName = f'Unlimited {modName[0].capitalize()} MTG'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # print(df_allDataIndividual)
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/unlimited/duals/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)
        
# begin revised routes
@app.route('/revised/stats/duals/<cardName>')
def renderIndividualRevisedCard(cardName):
    priceLegend = 'Price (avg)'
    lengthLegend = 'Avg Length (days)'
    countLegend = 'Total Sold (listings)'
    sumLegend = 'Cumulative Sales (gross)'
    spotLegend = 'Spot Price'
    depthLegend = 'Active Depth'
    headers = {'Content-Type': 'text/html'}
    modName = cardName.replace('%20', ' ').replace('-', ' ').split(' ')
    wordLength = len(modName)
    if wordLength > 1:
        modCardName = f'Revised {modName[0].capitalize()} {modName[1].capitalize()} MTG'
    else:
        modCardName = f'Revised {cardName.capitalize()} MTG'
    # cardName = cardName.split('-')
    # try:
    #     modCardName = f'Revised {cardName[0].capitalize()} {cardName[1].capitalize()} MTG'
    # except:
    #     modCardName = f'Revised {cardName[0].capitalize()} MTG'
    ########################
    # begin general schema #
    ########################
    df_allDataIndividual = get_all_data_individual_general(modCardName)
    # cardNameIndividual = list(df_allDataIndividual['completed_product_nick'])
    # cardTitleIndividual = list(df_allDataIndividual['completed_product_titles'])
    cardPriceIndividual = list(df_allDataIndividual['completed_product_prices'])
    # cardThumbIndividual = list(df_allDataIndividual['completed_product_img_thumb'])
    # cardHrefIndividual = list(df_allDataIndividual['completed_product_img_url'])
    # cardTypeIndividual = list(df_allDataIndividual['completed_product_lst_type'])
    # cardLocIndividual = list(df_allDataIndividual['completed_product_loc'])
    # cardStartIndividual = list(df_allDataIndividual['completed_product_start'])
    cardEndIndividual = list(df_allDataIndividual['enddate'])
    cardEndMonthIndividual = list(df_allDataIndividual['month'])
    cardEndDayIndividual = list(df_allDataIndividual['day'])
    # cardTimestampIndividual = list(df_allDataIndividual['timestamp'])
    # cardDepthIndividual = list(df_allDataIndividual['completed_product_depth'])
    cardDateIndividual = [f'{str(x).rstrip()} {float(str(y).lstrip()):.0f}' for x, y in zip(cardEndMonthIndividual, cardEndDayIndividual)]
    ######################
    # begin stats schema #
    ######################
    # begin completed
    df_allStatsIndividual = get_all_data_individual_stats(modCardName)
    cardStatsAvgIndividual = list(df_allStatsIndividual['completed_product_avg'])
    cardStatsLengthIndividual = list(df_allStatsIndividual['completed_product_avg_length'])
    cardStatsCountIndividual = list(df_allStatsIndividual['completed_product_depth'])
    cardStatsSumIndividual = list(df_allStatsIndividual['completed_product_sum'])
    cardStatsTimestampIndividual = list(df_allStatsIndividual['timestamp'])
    # begin active
    df_allActiveStatsIndividual = get_all_data_individual_stats_active(modCardName)
    # cardStatsAvgIndividual = list(df_allStatsIndividual['active_product_avg'])
    # cardStatsLengthIndividual = list(df_allStatsIndividual['active_product_avg_length'])
    cardStatsActiveCountIndividual = list(df_allActiveStatsIndividual['active_product_depth'])
    # cardStatsSumIndividual = list(df_allStatsIndividual['active_product_sum'])
    cardStatsActiveTimestampIndividual = list(df_allActiveStatsIndividual['timestamp'])
    # fetch JSON data from api endpoint
    url = "http://127.0.0.1:8050/api/revised/duals/table"
    # json_data = requests.get(url, timeout=15).json()
    json_data = requests.get(url).json()
    x = json_data['results']
    # iterate over parsed JSON results
    price = [i['price'] for i in x if modCardName == i['nick']]
    priceChange = [i['priceChange'] for i in x if modCardName == i['nick']]
    count = [i['count'] for i in x if modCardName == i['nick']]
    countChange = [i['countChange'] for i in x if modCardName == i['nick']]
    length = [i['length'] for i in x if modCardName == i['nick']]
    lengthChange = [i['lengthChange'] for i in x if modCardName == i['nick']]
    cumSum = [i['cumSum'] for i in x if modCardName == i['nick']]
    cumSumChange = [i['sumChange'] for i in x if modCardName == i['nick']]
    return make_response(render_template('individual_card.html',
        priceLegend=priceLegend, lengthLegend=lengthLegend, countLegend=countLegend, sumLegend=sumLegend, spotLegend=spotLegend, depthLegend=depthLegend,
        cardName=modCardName.split('MTG')[0], cardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[0], secondCardNameIndividual=modCardName.split('MTG')[0].split(' ', 1)[1].rstrip(),
        cardPriceIndividual=cardPriceIndividual, cardEndIndividual=cardEndIndividual, cardDateIndividual=cardDateIndividual,
        cardStatsAvgIndividual=cardStatsAvgIndividual, cardStatsLengthIndividual=cardStatsLengthIndividual, cardStatsCountIndividual=cardStatsCountIndividual, cardStatsSumIndividual=cardStatsSumIndividual, cardStatsTimestampIndividual=cardStatsTimestampIndividual,
        cardStatsActiveCountIndividual=cardStatsActiveCountIndividual, cardStatsActiveTimestampIndividual=cardStatsActiveTimestampIndividual,
        price=price, priceChange=priceChange, count=count, countChange=countChange, length=length, lengthChange=lengthChange, cumSum=cumSum, cumSumChange=cumSumChange,
        ), 200, headers)

@app.route("/search", methods=['GET'])
def search():
    # cursor = g.con.cursor()
    # cursor.execute('SELECT * FROM nutzer, kategorien, rezepte WHERE Nutzername OR Titel = %s', (request.form["search"],))
    # result = cursor.fetchall()
    # cursor.close()
    # print(request.form['name'])
    # data = requests.get()
    # print(data)
    # print(query)
    name = request.args.get('search')
    print(name)
    print('test')
    return 'test!'
    try:
        return redirect("http://www.example.com", code=302)
    except Exception as e:
        print(e)
        return 'temp 404 :/'
    #return render_template('Results.html', result = result)  

@app.route('/location/')
# @app.route('/location/<int:id>/')
def location():
    # return redirect(url_for('location', id=request.args['info']))
    query = request.args.get('search')
    if len(query) > 1:
        set = query.split(' ')[0].lower()
        cardName = query.split(' ', 1)[1].title()  # split only on the first occurence (to avoid filter words with spaces, e.g. Black Lotus)
        if cardName in ('Tundra', 'Underground Sea', 'Badlands', 'Taiga', 'Savannah', 'Scrubland', 'Volcanic Island', 'Bayou', 'Plateau', 'Tropical Island'):
            return redirect(url_for(f'renderIndividual{set.capitalize()}Card', cardName=cardName))
        elif cardName in ('Black Lotus', 'Mox Jet', 'Mox Ruby', 'Mox Emerald', 'Mox Sapphire', 'Mox Pearl', 'Timetwister', 'Time Walk', 'Ancestral Recall'):
            return redirect(url_for(f'renderIndividual{set.capitalize()}CardPower', cardName=cardName))
    else:
        return 'temp 404'

@app.route('/email', methods=['POST'])
def email():
    # TODO: add proper form handling, insert into db, etc etc @ 10/6/2018
    # deal with inserted emails here...insert into db properly
    # get form data
    print('logged email')
    data = request.values
    # print(data)
    file = open("emails.txt", "a") 
    file.write(f"{data['name']}: '{data['email']}'\n") 
    file.close() 
    # flash('Succesfully logged email!')
    return redirect(url_for('homepage'))

###################################
# begin wholly-rendered endpoints #
###################################
# begin general endpoints
class HomePage(Resource):
    def __init__(self):
        pass
    def get(self):
        headers = {'Content-Type': 'text/html'}
        # TODO: if we feed in, it's rendered with the page and thus only loaded in `once`. 
        # if we want the data to update/etc, we should use ajax calls to our rest api.
        return make_response(render_template('home.html',
            # alphaDataAvg=alphaDataAvg, alphaDataAvgTimestamp=alphaDataAvgTimestamp,
            # betaDataAvg=betaDataAvg, betaDataAvgTimestamp=betaDataAvgTimestamp,
            # unlimitedDataAvgPower=unlimitedDataAvgPower, unlimitedDataAvgTimestampPower=unlimitedDataAvgTimestampPower,
            # legend=legend
            get_cumulative_power=get_cumulative_power(), get_cumulative_count_power=get_cumulative_count_power(),
            get_cumulative_duals=get_cumulative_duals(), get_cumulative_count_duals=get_cumulative_count_duals()),
            # get_cumulative_power_yesterday=get_cumulative_power_yesterday(), get_cumulative_duals_yesterday=get_cumulative_duals_yesterday(),
            # get_cumulative_count_power_today=get_cumulative_count_power_today(), get_cumulative_count_power_yesterday=get_cumulative_count_power_yesterday(),
            # get_cumulative_count_duals_today=get_cumulative_count_duals_today(), get_cumulative_count_duals_yesterday=get_cumulative_count_duals_yesterday()),
            200, headers)

class Active(Resource):
    def __init__(self):
        pass
    def get(self):
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('active.html'), 200, headers)  
                  
class About(Resource):
    def __init__(self):
        pass
    def get(self):
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('about.html'), 200, headers)

class Footer(Resource):
    def __init__(self):
        pass
    def get(self):
        headers = {'Content-Type': 'text/html'}
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

# begin alpha API endpoints
class Alpha(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(alphaDataCountPower)) + 1
        minHeight = round(min(alphaDataCountPower)) - 10
        maxHeightLength = round(max(alphaDataLengthPower)) + 2
        minHeightLength = round(min(alphaDataLengthPower)) - 10
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('alpha.html',
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            # begin duals
            alphaDataAvgDuals=alphaDataAvgDuals[2::], alphaDataAvgTimestampDuals=alphaDataAvgTimestampDuals, 
            alphaDataLengthDuals=alphaDataLengthDuals, alphaDataLengthTimestampDuals=alphaDataLengthTimestampDuals, 
            alphaDataCountDuals=alphaDataCountDuals, alphaDataCountTimestampDuals=alphaDataCountTimestampDuals,
            alphaDataBreakdownNameDuals=alphaDataBreakdownNameDuals, alphaDataBreakdownavg=alphaDataBreakdownAvgDuals,
            alphaDataAllEndDuals=alphaDataAllEndDuals, alphaDataAllNameDuals=alphaDataAllNameDuals, alphaDataAllHrefDuals=alphaDataAllHrefDuals, alphaDataAllPriceDuals=alphaDataAllPriceDuals,
            alphaActiveDataAllStartDuals=alphaActiveDataAllStartDuals, alphaActiveDataAllNameDuals=alphaActiveDataAllNameDuals, alphaActiveDataAllHrefDuals=alphaActiveDataAllHrefDuals, alphaActiveDataAllPriceDuals=alphaActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            alphaDataCumulativePriceDuals=alphaDataCumulativePriceDuals, alphaDataCumulativeTimestampDuals=alphaDataCumulativeTimestampDuals, 
            # begin power
            alphaDataAvgPower=alphaDataAvgPower[2::], alphaDataAvgTimestampPower=alphaDataAvgTimestampPower, 
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
            # return jsonify({'results': []})
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
            # print(df_alphaIndividualCardCompletedStats)
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

# begin beta API endpoints
class Beta(Resource):
    def __init__(self):
        pass
    def get(self):
        maxHeight = round(max(betaDataCountPower)) + 1
        minHeight = round(min(betaDataCountPower)) - 10
        maxHeightLength = round(max(betaDataLengthPower)) + 2
        minHeightLength = round(min(betaDataLengthPower)) - 10
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('beta.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            # begin duals
            betaDataAvgDuals=betaDataAvgDuals[15::], betaDataAvgTimestampDuals=betaDataAvgTimestampDuals, 
            betaDataLengthDuals=betaDataLengthDuals, betaDataLengthTimestampDuals=betaDataLengthTimestampDuals, 
            betaDataCountDuals=betaDataCountDuals, betaDataCountTimestampDuals=betaDataCountTimestampDuals,
            betaDataBreakdownNameDuals=betaDataBreakdownNameDuals, betaDataBreakdownavg=betaDataBreakdownAvgDuals,
            betaDataAllEndDuals=betaDataAllEndDuals, betaDataAllNameDuals=betaDataAllNameDuals, betaDataAllHrefDuals=betaDataAllHrefDuals, betaDataAllPriceDuals=betaDataAllPriceDuals,
            betaActiveDataAllStartDuals=betaActiveDataAllStartDuals, betaActiveDataAllNameDuals=betaActiveDataAllNameDuals, betaActiveDataAllHrefDuals=betaActiveDataAllHrefDuals, betaActiveDataAllPriceDuals=betaActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            betaDataCumulativePriceDuals=betaDataCumulativePriceDuals, betaDataCumulativeTimestampDuals=betaDataCumulativeTimestampDuals, 
            # begin power
            betaDataAvgPower=betaDataAvgPower[8::], betaDataAvgTimestampPower=betaDataAvgTimestampPower, 
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
            # return jsonify({'results': []})
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
            # print(df_betaIndividualCardCompletedStats)
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
        return make_response(render_template('unlimited.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength, countLegend=countLegend, lengthLegend=lengthLegend, sumLegend=sumLegend, powerLegend=powerLegend,
            # begin duals
            unlimitedDataAvgDuals=unlimitedDataAvgDuals[7::], unlimitedDataAvgTimestampDuals=unlimitedDataAvgTimestampDuals, 
            unlimitedDataLengthDuals=unlimitedDataLengthDuals, unlimitedDataLengthTimestampDuals=unlimitedDataLengthTimestampDuals, 
            unlimitedDataCountDuals=unlimitedDataCountDuals, unlimitedDataCountTimestampDuals=unlimitedDataCountTimestampDuals,
            unlimitedDataBreakdownNameDuals=unlimitedDataBreakdownNameDuals, unlimitedDataBreakdownavg=unlimitedDataBreakdownAvgDuals,
            unlimitedDataAllEndDuals=unlimitedDataAllEndDuals, unlimitedDataAllNameDuals=unlimitedDataAllNameDuals, unlimitedDataAllHrefDuals=unlimitedDataAllHrefDuals, unlimitedDataAllPriceDuals=unlimitedDataAllPriceDuals,
            unlimitedActiveDataAllStartDuals=unlimitedActiveDataAllStartDuals, unlimitedActiveDataAllNameDuals=unlimitedActiveDataAllNameDuals, unlimitedActiveDataAllHrefDuals=unlimitedActiveDataAllHrefDuals, unlimitedActiveDataAllPriceDuals=unlimitedActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            unlimitedDataCumulativePriceDuals=unlimitedDataCumulativePriceDuals, unlimitedDataCumulativeTimestampDuals=unlimitedDataCumulativeTimestampDuals, 
            # begin power
            unlimitedDataAvgPower=unlimitedDataAvgPower[3::], unlimitedDataAvgTimestampPower=unlimitedDataAvgTimestampPower, 
            unlimitedDataLengthPower=unlimitedDataLengthPower, unlimitedDataLengthTimestampPower=unlimitedDataLengthTimestampPower, 
            unlimitedDataCountPower=unlimitedDataCountPower, unlimitedDataCountTimestampPower=unlimitedDataCountTimestampPower,
            unlimitedDataAllEndPower=unlimitedDataAllEndPower, unlimitedDataAllNamePower=unlimitedDataAllNamePower, unlimitedDataAllHrefPower=unlimitedDataAllHrefPower, unlimitedDataAllPricePower=unlimitedDataAllPricePower,
            unlimitedActiveDataAllStartPower=unlimitedActiveDataAllStartPower, unlimitedActiveDataAllNamePower=unlimitedActiveDataAllNamePower, unlimitedActiveDataAllHrefPower=unlimitedActiveDataAllHrefPower, unlimitedActiveDataAllPricePower=unlimitedActiveDataAllPricePower,
            unlimitedDataCumulativePricePower=unlimitedDataCumulativePricePower, unlimitedDataCumulativeTimestampPower=unlimitedDataCumulativeTimestampPower,
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
                     #TODO: this => .strftime('%m-%d-%Y')      
                     #                      
class UnlimitedPowerActive(Resource):
    def __init__(self):
        pass
    def get(self):
        unlimitedActivePower = clean_to_json(df_unlimitedActiveAllPower, 'active')
        unlimitedActivePowerJSON = json.loads(unlimitedActivePower)
        try:
            return jsonify({'results': unlimitedActivePowerJSON})
            # return jsonify({'results': []})
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
            # print(df_unlimitedIndividualCardCompletedStats)
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
                # print(modName)
                if modName[0] == 'black':
                    modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()} MTG'
                else:
                    modCardName = f'Unlimited {modName[0].capitalize()} {modName[1].capitalize()}'
            else:
                modCardName = f'Unlimited {name.capitalize()}'
            # print(modCardName)
            df_unlimitedIndividualCardCompletedStats = get_all_data_individual_general(modCardName)
            # print(df_unlimitedIndividualCardCompletedStats)
            unlimitedPowerIndividualTable = clean_to_json(df_unlimitedIndividualCardCompletedStats, 'table')
            unlimitedPowerIndividualTableJSON = json.loads(unlimitedPowerIndividualTable)
            try:
                return jsonify({'results': unlimitedPowerIndividualTableJSON})
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
        return make_response(render_template('revised.html', 
            dualsLegend=dualsLegend, maxHeight=maxHeight, minHeight=minHeight, maxHeightLength=maxHeightLength, minHeightLength=minHeightLength,
            revisedDataAvgDuals=revisedDataAvgDuals[2::], revisedDataAvgTimestampDuals=revisedDataAvgTimestampDuals, 
            revisedDataLengthDuals=revisedDataLengthDuals, revisedDataLengthTimestampDuals=revisedDataLengthTimestampDuals, revisedDataLengthLegendDuals=revisedDataLengthLegendDuals,
            revisedDataCountDuals=revisedDataCountDuals, revisedDataCountTimestampDuals=revisedDataCountTimestampDuals, revisedDataCountLegendDuals=revisedDataCountLegendDuals,
            revisedDataBreakdownNameDuals=revisedDataBreakdownNameDuals, revisedDataBreakdownavg=revisedDataBreakdownAvgDuals,
            revisedDataAllEndDuals=revisedDataAllEndDuals, revisedDataAllNameDuals=revisedDataAllNameDuals, revisedDataAllHrefDuals=revisedDataAllHrefDuals, revisedDataAllPriceDuals=revisedDataAllPriceDuals,
            # revisedDataStatsNameDuals=revisedDataStatsNameDuals, revisedDataStatsAvgDuals=revisedDataStatsAvgDuals, revisedDataStatsDepthDuals=revisedDataStatsDepthDuals, revisedDataStatsLengthDuals=revisedDataStatsLengthDuals, revisedDataStatsSumDuals=revisedDataStatsSumDuals,
            revisedActiveDataAllStartDuals=revisedActiveDataAllStartDuals, revisedActiveDataAllNameDuals=revisedActiveDataAllNameDuals, revisedActiveDataAllHrefDuals=revisedActiveDataAllHrefDuals, revisedActiveDataAllPriceDuals=revisedActiveDataAllPriceDuals,
            get_percent_change_last_sold=get_percent_change_last_sold, get_premiums=get_premiums, get_count=get_data_single_product_count_90, get_length=get_data_single_product_avg_length_90, get_depth=get_data_single_product_depth, 
            revisedDataCumulativePriceDuals=revisedDataCumulativePriceDuals, revisedDataCumulativeTimestampDuals=revisedDataCumulativeTimestampDuals, revisedDataCumulativeLegendDuals=revisedDataCumulativeLegendDuals,
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
                     #TODO: this => .strftime('%m-%d-%Y')      
                     #                      
class RevisedDualsActive(Resource):
    def __init__(self):
        pass
    def get(self):
        revisedActiveDuals = clean_to_json(df_revisedActiveAllDuals, 'active')
        revisedActiveDualsJSON = json.loads(revisedActiveDuals)
        try:
            return jsonify({'results': revisedActiveDualsJSON})
            # return jsonify({'results': []})
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
            # print(df_revisedIndividualCardCompletedStats)
            revisedDualsIndividualTable = clean_to_json(df_revisedIndividualCardCompletedStats, 'table')
            revisedDualsIndividualTableJSON = json.loads(revisedDualsIndividualTable)
            try:
                return jsonify({'results': revisedDualsIndividualTableJSON})
            except Exception as e:
                return jsonify({'results': 'failed'},
                            {'error': e})

################################
# begin general html endpoints #
################################
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
# begin revised api endpoints
api.add_resource(RevisedDualsIndexAverage, '/api/revised/duals/index/avg')
api.add_resource(RevisedDualsActive, '/api/revised/duals/active')
api.add_resource(RevisedDualsTable, '/api/revised/duals/table')
api.add_resource(RevisedIndividualCardCompletedStats, '/api/revised/duals/<name>')
# begin general api endpoints
api.add_resource(GeneralIndexAverage, '/api/general/index')


# initalize application
if __name__ == "__main__":
    # TEMPLATES_AUTO_RELOAD = True
    app.run(debug=True, port=8050, threaded=True)