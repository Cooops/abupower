from .build_dataframes import *


def get_percent_change_last_sold(name, value, boolCheck):
    if boolCheck == True:
        newValue = get_data_single_product_avg(name)
        calc = ((float(value.replace(',','')) - float(newValue[0])) / float(newValue[0])) * 100
        
        if calc == 0:
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
    alphaAverage = get_data_alpha_breakdown()
    betaAverage = get_data_beta_breakdown()
    unlimitedAverage = get_data_unlimited_breakdown()

    alphaToBetaPremium = calc_premiums(list(alphaAverage['completed_product_avg'].values), betaAverage['completed_product_avg'].values)
    alphaToUnlimitedPremium  = calc_premiums(alphaAverage['completed_product_avg'].values, unlimitedAverage['completed_product_avg'].values)
    betaToAlphaPremium  = calc_premiums(betaAverage['completed_product_avg'].values, alphaAverage['completed_product_avg'].values)
    betaToUnlimitedPremium  = calc_premiums(betaAverage['completed_product_avg'].values, unlimitedAverage['completed_product_avg'].values)
    unlimitedToAlphaPremium  = calc_premiums(unlimitedAverage['completed_product_avg'].values, alphaAverage['completed_product_avg'].values)
    unlimitedToBetaPremium  = calc_premiums(unlimitedAverage['completed_product_avg'].values, betaAverage['completed_product_avg'].values)

    return {'alphaToBeta': alphaToBetaPremium, 
            'alphaToUnlimited': alphaToUnlimitedPremium, 
            'betaToAlpha': betaToAlphaPremium, 
            'betaToUnlimited': betaToUnlimitedPremium,
            'unlimitedToAlpha': unlimitedToAlphaPremium,
            'unlimitedToBeta': unlimitedToBetaPremium,}

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

# begin memos
avgMemo = {}
lenMemo = {}
countMemo = {}
sumMemo = {}
spreadMemo = {}
avgActiveMemo = {}
depthMemo = {}
depth = []
    
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
        # begin zipping values before serializing to JSON (aka `dumping`)
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
        end = dataframe['enddate']
        url = dataframe['completed_product_img_url'].values
        thumb = dataframe['completed_product_img_thumb'].values
        type = dataframe['completed_product_lst_type'].values
        loc = dataframe['completed_product_loc'].values
        regLength = dataframe['length']
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
