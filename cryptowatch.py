""" project2.py

Complete the sections below marked with either
- '<COMPLETE THIS PART>'
- '<your answer here>'

         
"""


import os
import datetime as dt
import pprint as pp
import json
import requests
import pandas as pd


# ---------------------------------------------------------------------------- 
#   Locations 
# ---------------------------------------------------------------------------- 
# PRJDIR is the folder containing <Zid> folder you received as a zip file
# See the project2.pdf for a description
PRJDIR = r'C:\Users\steve\PycharmProjects\yfinance_downloader\z5194925'
FILESDIR = os.path.join(PRJDIR, 'files')
TESTDIR = os.path.join(PRJDIR, '_testfiles')

# This is where the file containing the exchange information will be saved
EXCHANGES = os.path.join(FILESDIR, 'exchanges.json')

# ---------------------------------------------------------------------------- 
#   Testing configuration
# ---------------------------------------------------------------------------- 
if not os.path.exists(FILESDIR):
    raise Exception(f'Cannot find the folder {FILESDIR}, please set `PRJDIR` ')



# ---------------------------------------------------------------------------- 
#   Do not change these constants!
# ---------------------------------------------------------------------------- 
# This list contains the exchange symbols you will consider in your study
EXCH_LST = [
        'kraken',
        'dex-aggregated', 
        'btc-china',      
        'coinbase-pro',
        'bitfinex',
        ]
# Period used in the analysis
PERIOD = '1m'
# This is the crypto of interest
SYMBOL = 'btcusd'


# ----------------------------------------------------------------------------
#   ANSWERS TO PART 1 QUESTIONS
#   No partial credit
# ----------------------------------------------------------------------------
# Instructions:
# - Please answer the questions from Part 1 of the document project2.pdf below
#
# - Your answers should be python strings.
#
#   - Answers are not case-sensitive, so 'Yes', 'yes', 'YeS' will all be
#     understood as 'yes'
#   - You can write strings in any way you want. So 'yes', "yes", and
#     """yes""" are all valid.
#   - You can answer numeric questions with strings if you want. So 1, '1',
#     '1.0', 1.0, will all be interpreted as 1.
#
# - Short answers should be concise (a couple of lines at most).
#
# - Do not change the name of the variables that will hold your answers.
#

# EXAMPLE: NOT REAL QUESTION:
# Here is an example:
# Question: What is 1 + 1?
Q100 = '<your answer here>'
# This means you should CHANGE the variable `Q100` from '<your answer here>'
# to
Q100 = '2'
# When assessing your code, we will parse the value of the variable Q100. If
# the answer is anything that can be interpreted as a number 2, you will get
# full mark. Otherwise, you will get 0 (there is no partial credit)

# -------------------------------
# Topic: Rate limit
# Src: https://docs.cryptowat.ch/rest-api/rate-limit
# -------------------------------

# Question 1: Do you *need* an API key for free/anonymous access?
# (Yes/No)
Q1 = 'No'

# Question 2: What is the initial value of your allowance in Cryptowatch
# credits? (Integer)
Q2 = '10'

# Question 3: Do all requests cost the same in credits? (Yes/No)
Q3 = 'No'

# -------------------------------
# Topic: Assets
# Src: https://docs.cryptowat.ch/rest-api/assets
# -------------------------------

# Question 4: What an asset? (max 150 characters)
Q4 = 'An asset is something that is traded, like a crypto or fiat currency.'

# Question 5: What is the *symbol* for the Bitcoin asset? (string)
Q5 = 'btc'

# -------------------------------
# Topic: Pairs
# Src: https://docs.cryptowat.ch/rest-api/pairs
# -------------------------------

# Question 6: What is a *pair*? (max 150 characters)
Q6 = 'A pair consists of a base and quote, and is traded on an exchange. The quote currency is how much is needed to purchase one unit of the base currency.'

# Question 7: What is the symbol for the USD price of one Bitcoin (i.e.,
# the Bitcoin, USD pair)? (string)
Q7 = 'btcusd'

# -------------------------------
# Topic: Markets
# Src: https://docs.cryptowat.ch/rest-api/markets
# -------------------------------

# Question 8: What is a *market*? (max 150 characters)
Q8 = 'A market is a pair listed on an exchange. For example, btceur on exchange kraken is a market.'

# Question 9: What is the market endpoint URL for the pair 'btcusd' and
# exchange 'kraken'? (URL)
Q9 = 'https://api.cryptowat.ch/markets/kraken/btcusd'

# Question 10: In which format is the raw data represented? (string)
Q10 = 'json'

# Question 11: Based on the data contained in the response, is this
# market *active*?  In other words, can you use USD to trade bitcoins in
# this exchange? (Yes/No)
Q11 = 'yes'

# Question 12: Based on the data contained in the response, what is the
# *id* for this market? This is the integer Cryptowatch uses to identify
# this market. (integer)
Q12 = '87'

# -------------------------------
# Topic: OHLC
# Src: https://docs.cryptowat.ch/rest-api/markets/ohlc
# -------------------------------

# Question 13: What is the endpoint URL for the OHLC market represented
# by the *Kraken* exchange and the *btcusd* pair? Assume that you do
# **not** have any optional query parameters. (URL)
Q13 = 'https://api.cryptowat.ch/markets/kraken/btcusd/ohlc'

# Question 14: For the resources returned by OHLC endpoints (without
# query parameters), what is the length of the interval for the candles
# stored under the JSON key 14400?  Hint The page 
#  https://docs.cryptowat.ch/rest-api/markets/ohlc
# contains an example of a response to a request with an endpoint
# similar to the one above. You can use this response and the *period
# table* at the end of this page:
#   https://docs.cryptowat.ch/rest-api/markets/ohlc) 
# to answer this question. (string)
Q14 = '4h'

# Question 15: Suppose you are given the following *candle* in a string:
# [1474736400, 8744, 8756.1, 8710, 8753.5, 91.58314308, 799449.488966417
# ] Which of these values correspond to the close quote? (number)
Q15 = '8753.5'

# Question 16: In the candle above, the unix timestamp is `1474736400`.
# Use the `datetime` module to answer the following question: What is
# the **UTC** date and time corresponding to this timestamp? (Hint: once
# you create the datetime instance, you can use the `print` function and
# use the output as your answer) (string)
Q16 = '2016-09-24 17:00:00'







# ---------------------------------------------------------------------------- 
#   Auxiliary functions
# ---------------------------------------------------------------------------- 
def to_json(obj, pth):
    """ Converts the object `obj` into a formatted JSON object and save it to
    a file located at `pth`.

    Parameters
    ----------
    obj : a python object
        Object to be serialized. 

    pth : str
        Location where the JSON with be saved.
        **IF THE FILE `pth` EXISTS, IT WILL BE OVERWRITTEN!!!**

    Returns
    -------
    None

    Notes
    -----

    This function uses the method `json.dump` to convert the object to JSON
    formatted string. See the docs for more information about this method:

    <https://docs.python.org/3/library/json.html#json.dump> for more
    
    The output will be formatted as follows:

    - The output of dictionaries will be sorted by key

    - JSON array elements and object members will be pretty-printed with 
      an indention level of `4`

    Examples
    --------
    >> obj = {'a': 1, 'b': [4, 5, 6]}
    >> pth = 'test.json'
    >> to_json(obj, pth)

    This will create a file called `test.json` with the following contents:
    
    {
        "a": 1,
        "b": [
            4,
            5,
            6
        ]
    }

    """
    # <COMPLETE THIS PART>
    with open(pth, "w") as file:
        json.dump(obj=obj, fp=file, indent=4)

def from_json(pth):
    """ Converts the JSON formatted contents of the file in `pth` to a Python
    object.

    Parameters
    ----------
    pth : str
        Location of the file containing the JSON stream

    Returns
    -------
    obj
        
        A Python object with the converted JSON stream according to this
        table: <https://docs.python.org/3/library/json.html#json-to-py-table>

    Notes
    -----

    - This function uses the method `json.load` to convert the JSON stream. See
      <https://docs.python.org/3/library/json.html#json.load>

    Examples
    --------
    Suppose the contents of the `test.json` file are:
    {
        "a": 1,
        "b": [
            4,
            5,
            6
        ]
    }


    >> pth = 'test.json'
    >> dic = from_json(pth)
    >> print(dic)
    {'a': 1, 'b': [4, 5, 6]}


    """
    # <COMPLETE THIS PART>
    with open(pth, "r") as f:
        return json.load(fp=f)


def get_ohlc_loc(exchange, symbol):
    """ Returns the location of where the FILESDIR data will be saved 

    Parameters
    ----------
    exchange : str
        The symbol corresponding to this exchange. This information is
        contained in the `exchanges.json` file you downloaded above. For
        instance, the symbol for the Kraken exchange is "kraken".

    symbol : str
        The symbol representing the pair or assets. For instance, the symbol
        for the AUD price of a bitcoin will be "btcaud". You can find more
        information about pairs here
        <https://docs.cryptowat.ch/rest-api/pairs>

    Returns
    -------
    str

        String with the location of the file containing the FILESDIR data for this
        market. The name of the file will be a combination of the symbols for
        the exchange and the pair. For instance, if `exchange="kraken"` and
        `symbol="btcaud"`, the name of the file will be `kraken_btcaud.json`,
        and it will be saved inside the folder `FILESDIR`.


    Examples
    --------
    >> pth = get_ohlc_loc(exchange='kraken', symbol='btcaud')
    >> print(pth)
    '<path to FILESDIR/kraken_btcaud.json'


    """
    # <COMPLETE THIS PART>
    exch_symbol = exchange + "_" + symbol + ".json"
    pth = os.path.join(FILESDIR, exch_symbol)
    return pth



# ---------------------------------------------------------------------------- 
#   (*)  Download exchange information. Save it to a text file
# ---------------------------------------------------------------------------- 
def write_exchanges():
    """ This function will request a list of exchanges from Cryptowatch and
    save the data contained in the response to a file. The location of this
    file is given by the constant `EXCHANGES`
    
    
    This function will perform the following operations:
    
    1. Given the endpoint defined below, send a request and obtain the
        response.

    2. Retrieve the JSON formatted string containing the data and save it in
        the file located at `EXCHANGES`


    Notes
    -----
    - You should use the variable `EXCHANGES` somewhere inside this function

    - The value of the EXCHANGES constant is already defined at the beginning
      of this module. Do not change it. Just make sure this function saves the
      JSON string to this file.

    - You can find information about exchange endpoints here:
        <https://docs.cryptowat.ch/rest-api/exchanges>

    - You may use the function `to_json` you created above if you want.

    - The file located at `EXCHANGES` is a simple text file with a `.json`
      extension.  You should open this file and take a look at its contents
      once you save it.

    Examples
    --------
    >> write_exchanges()

    Example of the contents of the `EXCHANGES` file:

    {
      "result": [
        {
          "id": 17,
          "symbol": "mexbt",
          "name": "meXBT",
          "route": "https://api.cryptowat.ch/exchanges/mexbt",
          "active": false
        },
        {
          "id": 62,
          "symbol": "coinone",
          "name": "Coinone",
          "route": "https://api.cryptowat.ch/exchanges/coinone",
          "active": true
        },
        // ...
      ]
    }

    """
    # Here is the endpoint that you need to use:
    endpoint = 'https://api.cryptowat.ch/exchanges'
    # <COMPLETE THIS PART> 
    res = requests.get(endpoint)
    dic = res.json()
    to_json(dic, EXCHANGES)


# ---------------------------------------------------------------------------- 
#   Read the file saved by write_exchange
# ---------------------------------------------------------------------------- 
def read_exchanges():
    """ Reads the `EXCHANGES` file (output of `write_exchanges`)
        and returns a dataframe with the relevant information. 


    Returns
    -------
    df
        A dataframe with the following structure: 

        df.index: The values of the index should correspond to the "id" of the
            exchange. The "id" is included in the JSON file containing information
            about the exchanges.

        df.columns: The columns should be (the order is not important):

            Data columns (total 4 columns):
             #   Column  Non-Null Count  Dtype 
            ---  ------  --------------  ----- 
             0   active  XXX non-null     bool  
             1   name    XXX non-null     object
             2   route   XXX non-null     object
             3   symbol  XXX non-null     object
            dtypes: bool(1), object(3)


    Notes
    -----
    - You should use the variable `EXCHANGES` somewhere inside this function

    - The file located in `EXCHANGES` is a simple text file. You should look at its
      contents before writing this function.

    - You may use the function `from_json` to convert the contents of
      `EXCHANGES` to a Python object. You can then use a modified version of
      this object to produce the dataframe.

    Examples
    --------
    >> df = read_exchanges()
    >> df
         active  ... symbol
    id           ...       
    12     True  ... okcoin 
    ...     ...       ...

    """
    # <COMPLETE THIS PART>
    data = from_json(EXCHANGES)['result']
    df = pd.DataFrame(data)
    df.set_index('id', inplace=True)
    cols = ['active', 'name', 'route', 'symbol']
    return df[cols]






# ---------------------------------------------------------------------------- 
#   (*)  Create a function that retrieves the ohlc for a ticker in a given
#   market by passing the following parameters
# ---------------------------------------------------------------------------- 
def write_ohlc(exchange, symbol, filesdir=None):
    """ Fetches the open, high, low, and close quotes for a given market
    (combination of `exchange` and `symbol`) and  saves the contents of the
    response to a file.

    Parameters
    ----------
    exchange : str
        The symbol corresponding to this exchange. This information is
        contained in the `exchanges.json` file you downloaded above. For
        instance, the symbol for the Kraken exchange is "kraken".

    symbol : str
        The symbol representing the pair or assets. For instance, the symbol
        for the AUD price of a bitcoin will be "btcaud". You can find more
        information about pairs here
        <https://docs.cryptowat.ch/rest-api/pairs>


    Notes
    -----

    You may follow the following steps if you want (although not
    necessary)

    Step 1: Create the correct endpoint for the resource given by
      `exchange` and `symbol`.
    Step 2: Send the request, obtain the response.
    Step 3: Retrieve the data contained in the response as a Python object
    Step 4: Save this object to a JSON file using the `to_json` function.

    Examples
    --------
    >> write_ohlc(exchange='kraken', symbol='btceur')

    This will save a file JSON text file containing something similar to this
    (actual values may be different, obviously):
    {
        "allowance": {
            "cost": 0.015,
            "remaining": 9.919,
            "upgrade": "For unlimited API access, create an account at https://cryptowat.ch"
        },
        "result": {
            "14400": [
                [
                    1590854400,
                    8590,
                    8600,
                    8546.7,
                    8577.1,
                    468.73767641,
                    4020936.473837751
                ],
        ... ... ... 
        ... ... ... 
    }


    """
    # <COMPLETE THIS PART>
    endpoint = 'https://api.cryptowat.ch/markets' + f"/{exchange}/{symbol}/ohlc"
    res = requests.get(endpoint)
    dic = res.json()
    ohlc_path = get_ohlc_loc(exchange, symbol)
    to_json(dic, ohlc_path)


def candle_to_dic(lst):
    """ Given a list containing FILESDIR data for a candle, 
    returns a dictionary with these data.

    Parameters
    ----------
    lst : list
        A 7-element list containing data for a candle. 

    Returns
    -------
    dict
        A dictionary with the content of the candle mapped to the correct key.
        This dictionary should include the following keys (in any order):
            - 'close'
            - 'high'
            - 'low'
            - 'open'
            - 'timestamp'
            - 'value'
            - 'volume'
        where: 
            - 'close': The close price of this candle (float),
            - 'high': The high price for this candle (float),
            - 'low': The low price for this candle (float),
            - 'open': The open price for this candle (float),
            - 'timestamp': integer with the UNIX timestamp (integer) at the end of the candle,
            - 'value': The value for the corresponding volume (float),
            - 'volume': The volume (float),

    Notes
    -----
    - See <https://docs.cryptowat.ch/rest-api/markets/ohlc> for more
      information

    Examples
    --------
    >> lst = [
    ...      1590854400,
    ...      8590,
    ...      8600,
    ...      8546.7,
    ...      8577.1,
    ...      468.73767641,
    ...      4020936.473837751
    ...      ]

    >> dic = candle_to_dic(lst)
    

    I will not print the output of `dic` in this example since the point of
    the exercise is to figure out how to map the keys to the values in the lst
    above.

    """
    # <COMPLETE THIS PART>
    keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'value']
    res = {keys[i]: lst[i] for i in range(len(keys))}
    for key, value in res.items():
        if key == 'timestamp':
            res[key] = int(value)
        else:
            res[key] = float(value)
    return res



def period_to_secs(period):
    """ Converts a valid value of `period` to the corresponding number 
    of seconds (as a string)

    Parameters
    ----------
    period : str
        Period of time between the start and end of the candle. Valid values
        are:
        - Minutes: '1m', '3m', '5m', '15m', '30m',
        - Hours: '1h', '2h', '4h', '6h', '12h',
        - Days/Weeks: '1d', '3d', '1w', 


    Returns
    -------
    str
        a STRING with the number of seconds corresponding to the period of
        time in `period`

    Notes
    -----

    - You can use datetime.timedelta objects to convert `period` to seconds.
      ALTERNATIVELY, you can simply create a dictionary that maps each valid
      value of `period` to the number of seconds.

    - You can find a conversion table between seconds and the valid labels for
      `period` here: 
        <https://docs.cryptowat.ch/rest-api/markets/ohlc#period-values> 
    
    """
    # Leave this here! It will check to make sure the parameter `period` is
    # valid
    _valid_values = [
        '1m', '3m', '5m', '15m', '30m',
        '1h', '2h', '4h', '6h', '12h',
        '1d', '3d', '1w', 
        ]
    if period not in _valid_values:
        raise Exception(f'Parm `period` must be in {",".join(_valid_values)}')


    # <COMPLETE THIS PART>
    secs = ['60', '180', '300', '900', '1800', '3600', '7200', '14400', '21600', '43200', '86400', '259200', '604800']
    res = {_valid_values[i]: secs[i] for i in range(len(_valid_values))}
    return res[period]



def read_ohlc(exchange, symbol, period):
    """ Reads the data located in the `exchage`_`symbol`.json file and returns
    a list of dictionaries with the candles for the period `period`


    Parameters
    ----------
    exchange : str
        The symbol corresponding to this exchange. This information is
        contained in the `exchanges.json` file you downloaded above. For
        instance, the symbol for the Kraken exchange is "kraken".

    symbol : str
        The symbol representing the pair or assets. For instance, the symbol
        for the AUD price of a bitcoin will be "btcaud". You can find more
        information about pairs here
        <https://docs.cryptowat.ch/rest-api/pairs>

    period : str
        Period of time between the start and end of the candle. Valid values
        are:
        - Minutes: '1m', '3m', '5m', '15m', '30m',
        - Hours: '1h', '2h', '4h', '6h', '12h',
        - Days/Weeks: '1d', '3d', '1w', 

    Returns
    -------
    list
        A list of dictionaries. Each element in this list is a dictionary with
        the following keys (in any order):
            - 'close'
            - 'high'
            - 'low'
            - 'open'
            - 'timestamp'
            - 'value'
            - 'volume'

    Notes
    -----

    You may follow the following steps if you want (although not
    necessary)

    - Step 1: Read the source file and return a dictionary with the data. You
        may use the `from_json` function.

    - Step 2: Figure out where in the dictionary the candles you need are
        located.  The function `period_to_secs` you created
        above might be helpful.

    - Step 3: Parse each candle using the `candle_to_dic` function above,
        combine the result, and return the result as a list of dictionaries

    Examples
    --------
    >> lst = read_ohlc(exchange="kraken", symbol="btceur", period='5m')
    >> dic = list[0]
    >> pp.pprint(dic)
    {
     'close': <some value>,
     'high': <some value>,
     'low': <some value>,
     'open': <some value>,
     'timestamp': <some value>,
     'value': <some value>,
     'volume': <some value>,
    }

    Note: The order of the keys above may be different than the one 
    produced by your code.


    """
    # Leave this here
    pth = get_ohlc_loc(exchange, symbol)


    # <COMPLETE THIS PART>
    lst = []
    converted_period = period_to_secs(period)
    nested_list = from_json(pth)['result'][converted_period]
    for x in nested_list:
        a = candle_to_dic(x)
        lst.append(a)
    return lst



def mk_ohlc_df(exchange, symbol, period):
    """ Creates a dataframe with the quotes for the pair `symbol` trading in 
    the exchange `exchange`, using a period-length of `period`. 
    This dataframe will include a column with the name of the exchange.

    Parameters
    ----------
    exchange : str
        The symbol corresponding to this exchange. This information is
        contained in the `exchanges.json` file you downloaded above. For
        instance, the symbol for the Kraken exchange is "kraken".

    symbol : str
        The symbol representing the pair or assets. For instance, the symbol
        for the AUD price of a bitcoin will be "btcaud". You can find more
        information about pairs here
        <https://docs.cryptowat.ch/rest-api/pairs>

    period : str
        Period of time between the start and end of the candle. Valid values
        are:
        - Minutes: '1m', '3m', '5m', '15m', '30m',
        - Hours: '1h', '2h', '4h', '6h', '12h',
        - Days/Weeks: '1d', '3d', '1w', 

    Returns
    -------
    dataframe
        A dataframe with the following structure: 

        df.index : A datetimeindex with the UTC datetimes corresponding to the
            UNIX timestamps in 'timestamp'

        df.columns: The column labels and dtypes should be (the order of the
        columns in the DF does NOT matter):

        Data columns (total 8 columns):
         #   Column     Non-Null Count  Dtype  
        ---  ------     --------------  -----  
         0   timestamp   non-null   int64  
         1   exchange    non-null   object 
         2   value       non-null   float64
         3   open        non-null   float64
         4   high        non-null   float64
         5   low         non-null   float64
         6   close       non-null   float64
         7   volume      non-null   float64

    Notes
    -----
    Suggested steps (but you can follow your own):
    
    - Step 1: Use the `read_ohlc` to get the data
    - Step 2: Create a dataframe with these data
    - Step 3: Create a column called "utc_dt" with the datetime objects
        corresponding to the integers (unix timestamp) in "timestamp". Make
        sure these dates use UTC, **NOT** local time. 
    - Step 4: Set column "utc_dt" as the index
    - Step 5: Create a column called "exchange", containing the string passed
      to the `exchange` parameter of this function.

    Examples
    --------
    >> df = mk_ohlc_df(exchange='kraken', symbol='btceur', period='5m')
    >> print(df[['timestamp', 'exchange']].iloc[0:1])
                          timestamp exchange
    utc_dt                                  
    2020-11-09 14:45:00  1604933100   kraken

    Note: The index and value above may be different than the one produced by
    your code. 
    """
    # This is step 1
    data = read_ohlc(exchange=exchange, symbol=symbol, period=period)
    df = pd.DataFrame(data)
    # <COMPLETE THIS PART>
    def convert_to_utc(x):
        return dt.datetime.utcfromtimestamp(x)
    df['utc_dt'] = df['timestamp'].apply(convert_to_utc)
    df.set_index('utc_dt', inplace=True)
    df['exchange'] = exchange
    return df




# ---------------------------------------------------------------------------- 
#   This function is given to you, no need to change it
# ---------------------------------------------------------------------------- 
def get_valid_exchanges():
    """ Returns a list of exchange symbols such that each symbol is:

        - in `EXCH_LST`
        - included in the file `EXCHANGES`
        - The value of `active` for this symbol in the EXCHANGES file is True

    Returns
    -------
    list
        List of exchange symbols as specified above

    Notes
    -----
    Hint: Use the function `read_exchanges` to get exchange information
    contained in the `EXCHANGES` file. Then check if the symbols in `EXCH_LST`
    have a corresponding "active" value of True.


    DO NOT MODIFY THE VALUES OF `EXCH_LST` specified at the beginning of this module.

    """
    df = read_exchanges()
    cond = df['active'] == True
    valid = set(df.loc[cond]['symbol'].values)
    return [x for x in EXCH_LST if x in valid]


# ---------------------------------------------------------------------------- 
#   Download all the required data
# ---------------------------------------------------------------------------- 
def get_all_data(valid_exch):
    """ For each exchange symbol in `valid_exch`, downloads the OHLC quotes
    for the pair `SYMBOL`.
    
    Notes
    -----
    For each value of `valid_exch`, call `write_ohlc` with the appropriate
    parameters.

    
    DO NOT MODIFY THE VALUES OF `EXCH_LST` or `SYMBOL`specified
    at the beginning of this module.

    """
    # <COMPLETE THIS PART>
    for exch in valid_exch:
        write_ohlc(exch, SYMBOL)
    


# ---------------------------------------------------------------------------- 
#   This function is already written, do not modify it
# ---------------------------------------------------------------------------- 
def compare_exchanges(valid_exch):
    """ For a given list of exchanges, compare prices and volume for the pair
    in `SYMBOL`.

    YOU DO NOT HAVE TO MODIFY ANYTHING IN THIS FUNCTION. JUST RUN IT

    Parameters
    ----------
    valid_exch : list
        A list of valid exchange symbols. See the function
        `get_valid_exchanges`

        
    """
    # -------------------------------------------------------- 
    #   Combine all dataframes
    # -------------------------------------------------------- 
    parms = {
            'symbol': SYMBOL,
            'period': PERIOD,
            }
    if len(valid_exch) == 1:
        print(f'Parm `valid_exch` contains only one symbol {valid_exch[0]}')
        print('comparison not possible')
        print('Exiting...')
        return None
    else:
        df = pd.concat([mk_ohlc_df(x, **parms) for x in valid_exch])

    # -------------------------------------------------------- 
    #   Reindex the DF so we can get unique row identifiers
    # -------------------------------------------------------- 
    df.reset_index(inplace=True)
    # Only a subset of cols, drop NA
    df = df.loc[:, ['exchange', 'timestamp', 'volume', 'close']].dropna()

    # -------------------------------------------------------- 
    #   Only interested in indexes (timestamps) available to all exchanges
    # -------------------------------------------------------- 
    nexch = len(valid_exch)
    groups = df.groupby('timestamp')
    df = groups.filter(lambda x : len(x) == nexch )

    groups = df.groupby('timestamp')

    # -------------------------------------------------------- 
    #   For each timestamp, select the exchange with the highest volume
    # -------------------------------------------------------- 
    idx_high_vol = groups['volume'].idxmax()
    # Create a column which take the value True if exchange had the highest
    # volume that timestamp
    df.loc[:, 'hi_vol'] = df.index.isin(idx_high_vol)

    # -------------------------------------------------------- 
    #   For each timestamp, select the exchange with the cheapest price
    # -------------------------------------------------------- 
    idx_low_prc = groups['close'].idxmin()
    df.loc[:, 'low_prc'] = df.index.isin(idx_low_prc)

    # -------------------------------------------------------- 
    #   Compute proportions by exchange
    # -------------------------------------------------------- 
    # Since True evaluates to 1, we can simply take the mean of this column
    # (by exchange) to tabulate the proportion of times 
    cols = ['hi_vol', 'low_prc']
    tab = df.groupby('exchange').mean()[cols]
    col_map = {
            'hi_vol': 'Prop of sample with highest volume',
            'low_prc': 'Prop of sample with lowest price',
            }
    tab.rename(columns=col_map, inplace=True)
    print(tab)




# ---------------------------------------------------------------------------- 
#  Testing functions (you can modify these if you want) 
# ---------------------------------------------------------------------------- 
def _test_to_json():
    """ Testing `to_json`. 
    You can modify this function if you want
    """
    obj = {'a': 1, 'b': [4, 5, 6]}
    pth = os.path.join(TESTDIR, 'test.json')
    to_json(obj, pth)

def _test_from_json():
    """ Testing `from_json`
    You can modify this function if you want
    """
    pth = os.path.join(TESTDIR, 'test.json')
    dic = from_json(pth)
    print(dic)

def _test_write_ohlc():
    """ Testing `write_ohlc`
    You can modify this function if you want
    """
    write_ohlc(exchange='kraken', symbol='btceur')

def _test_get_ohlc():
    """ Testing `write_ohlc`
    You can modify this function if you want
    """
    get_ohlc_loc(exchange='kraken', symbol='btceur')

def _test_candle_to_dic():
    """ Testing `write_ohlc`
    You can modify this function if you want
    """
    lst = [
         1590854400,
         8590,
         8600,
         8546.7,
         8577.1,
         468.73767641,
         4020936.473837751
         ]
    dic = candle_to_dic(lst)
    print(dic)

def _test_period_to_secs():
    seconds = period_to_secs('6h')
    print(seconds)

def _test_read_ohlc():
    """ Testing `read_ohlc`
    You can modify this function if you want
    """
    lst = read_ohlc(exchange='kraken', symbol='btceur', period='5m')
    pp.pprint(lst[0])

def _test_mk_ohlc_df():
    """ Testing `read_ohlc`
    You can modify this function if you want
    """
    df = mk_ohlc_df(exchange='kraken', symbol='btceur', period='5m')
    # print(df[['timestamp', 'exchange']].iloc[0:1])
    print(df)
    print(df.info())


# ---------------------------------------------------------------------------- 
#   Main function
# ---------------------------------------------------------------------------- 
def main(get_exchanges=True, get_ohlc=True):
    """
    IMPORTANT: You can comment parts of this function as much as you want.
    Just make sure you uncomment everything before calling main() for the
    final time
    """

    # -------------------------------------------------------- 
    #   Step 1: Download exchanges
    #   You only need to run the function `write_exchange` once. 
    #   After you have downloaded the `exchanges.json` file, you can call main
    #   with the parameter `get_exchanges` = False
    # -------------------------------------------------------- 
    if get_exchanges is True:
        print('-'*40)
        print('Set the parameter `get_exchanges` to False after')
        print('successfully downloading the exchange list')
        print('-'*40)
        write_exchanges()
   
    # -------------------------------------------------------- 
    # Step 1(b): Figure out which exchanges are valid
    #   Get valid exchanges
    # -------------------------------------------------------- 
    valid_exch = get_valid_exchanges()

    # -------------------------------------------------------- 
    # Step 2: Get all OHLC data
    #   You only need to run this function once
    # -------------------------------------------------------- 
    if get_ohlc is True:
        print('-'*40)
        print('Set the parameter `get_ohlc` to False after')
        print('successfully downloading the OHLC data')
        print('-'*40)
        get_all_data(valid_exch)

    # -------------------------------------------------------- 
    #  Step 3: Run the analysis
    #   (uncomment to run)
    # -------------------------------------------------------- 
    compare_exchanges(valid_exch)

    

# ---------------------------------------------------------------------------- 
#   test
# ---------------------------------------------------------------------------- 
if __name__ == "__main__":
    pass
    # --------------------------------------------------------     
    #   You can uncomment the test functions below during debug
    # -------------------------------------------------------- 
    # _test_to_json()
    # _test_from_json()
    # _test_write_ohlc()
    # _test_get_ohlc()
    # _test_candle_to_dic()
    # _test_period_to_secs()
    # _test_read_ohlc()
    # _test_mk_ohlc_df()

    # -------------------------------------------------------- 
    #   Main function
    # -------------------------------------------------------- 
    # Step 1:
    # main(get_exchanges=True, get_ohlc=False)

    # Step 2:
    # main(get_exchanges=False, get_ohlc=True)

    # Step 3: This will download all files again
    # main()
    

    
    











