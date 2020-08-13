from web3 import Web3
import json
import datetime
import sys
from utils import Contract, Event, Function, Token
import pandas as pd
import os

class UniswapHandler(object):
    def __init__(self):
        pass

    def update_logs(self, wanted=['DAI', 'HEX', 'MKR', 'LINK', 'REP', 'sETH', 'USDC', 'WBTC', 'WETH']):
        for name in wanted:
            ue = UniswapExchange(name, log=False)
            print(name)
            ue._collect_events(save=True)

    def update_NewExchange(self):
        fields = ['event', 'logIndex', 'transactionIndex', 'address', 'blockNumber', 'token', 'exchange']
        u = Uniswap(paid=True)
        logs_gen = u.get_events('NewExchange', fromBlock=6000000, toBlock=u.block, stride=5000)
        other = pd.DataFrame()
        for item in logs_gen:
            temp = pd.DataFrame(item)
            other = pd.concat((other, temp), ignore_index=True)
        to_save = other[fields]
        ne = to_save.to_dict(orient='records')
        pd.DataFrame({item['token']:item for item in ne}).to_json('../SilentRed/uniswap/data/NewExchange.json')

class Uniswap(Event, Function):
    def __init__(self, address='Factory', abi_path=None, abi=False, paid=False, log=False, local=False, ws=True):
        self._contract_addresses = {
            'Factory':'0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95'
        }
        Event.__init__(self,
            contract_address=self._contract_addresses[address],
            abi_path=abi_path if abi_path else f'uniswap/{address}.json',
            abi=abi,
            paid=paid,
            log=log,
            local=local,
            ws=ws
            )
        with open('../SilentRed/uniswap/data/NewExchange.json', 'r') as f:
            self.exchanges = json.load(f)
        with open('../SilentRed/uniswap/data/Tokens.json', 'r') as f:
            self.tokens = json.load(f)
        #self.exchange_count = self.functions.tokenCount().call()

    def update_tokens(self):
        for address in self.exchanges:
            if address not in self.tokens:
                try:
                    token_data = Token(address, log=False).data()
                    self.tokens[address] = token_data
                except Exception as e:
                    print(e)
        with open('../SilentRed/uniswap/data/Tokens.json', 'w') as f:
            json.dump(self.tokens, f)

class UniswapExchange(Event, Function):
    def __init__(self, address=None, abi_path=None, abi=False, paid=True, log=True, local=False, ws=True):
        self.parent = Uniswap(paid=paid, log=log, local=local)
        self.supported = {
            'DAI':'0x6B175474E89094C44Da98b954EedeAC495271d0F',
            'HEX':'0x2b591e99afE9f32eAA6214f7B7629768c40Eeb39',
            'MKR':'0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2',
            'LINK':'0x514910771AF9Ca656af840dff83E8264EcF986CA',
            'REP':'0x1985365e9f78359a9B6AD760e32412f4a445E862',
            'sETH':'0x5e74C9036fb86BD7eCdcb084a0673EFc32eA31cb',
            'USDC':'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
            'WBTC':'0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
            'WETH':'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        }
        address = self.supported.get(address, address)
        # exchange address passed
        token_address = self.parent.contract.functions.getToken(address).call()
        if token_address == '0x0000000000000000000000000000000000000000':
            # Not valid. Did they pass a token address?
            exchange_address = self.parent.functions.getExchange(address).call()
            if exchange_address == '0x0000000000000000000000000000000000000000':
                raise ValueError("Address not found.")
            else:
                self.exchange_address = exchange_address
                self.token_address = address
        else:
            self.exchange_address = address
            self.token_address = token_address
        exchange_address = self.exchange_address
        Event.__init__(self,
            contract_address=exchange_address,
            abi_path='uniswap/Exchange.json',
            abi=abi,
            paid=paid,
            log=log,
            local=local,
            ws=ws
            )
        self.token = Token(self.token_address, paid=paid, log=log, local=local)
        self.token_data = self.token.data()
        with open('../SilentRed/uniswap/data/EventMapping.json', 'r') as f:
            self.event_mapping = json.load(f)

    @property
    def get_earliest_block(self):
        return self.parent.exchanges[self.token_address]['blockNumber']

    @property
    def get_latest_block(self, event):
        filepath = '../SilentRed/uniswap/data/exchanges/' + self.token_data['symbol'] + '/' + event + '.csv'
        temp = pd.read_csv(filepath)
        return temp['blockNumber'].max()

    def _merge(self, old, new, index_name):
        merged = old.merge(new, how='outer').drop_duplicates(keep='last').sort_values(by=index_name).set_index(index_name).reset_index()
        return merged

    '''BALANCES'''
    def token_balance(self, address, block=None, **kwargs):
        if not block:
            block = self.block
        bal = self.token.functions.balanceOf(self.conn.toChecksumAddress(address)).call(block_identifier=block)
        return self._convert(bal, **kwargs)

    '''SUPPLY'''
    def eth_supply(self, **kwargs):
        return self.eth_balance(self.contract_address, **kwargs)

    def token_supply(self, **kwargs):
        return self.token_balance(self.contract_address, **kwargs)

    def liquidity_token_supply(self, block=None, **kwargs):
        if not block:
            block = self.block
        balance = self.contract.functions.totalSupply().call(block_identifier=block)
        return self._convert(balance, **kwargs)

    def liquidity_token_balance(self, add, block=None, **kwargs):
        bal = self.contract.functions.balanceOf(add)
        return self._convert(bal, **kwargs)

    @staticmethod
    def inputPrice(inr, our, a):
        return our*a*997/(inr*1000 + a*997)

    # def priceE(amounts):
    #     ethReserve = self.eth_supply()
    #     tokenReserve = self.token_supply()
    #     tokenDecimals = self.token_data['decimals']
    #     ethDecimals = 18
    #     price = (tokenReserve/10**tokenDecimals)/(ethReserve/10**ethDecimals)
    #     return [[(inputPrice(ethReserve, tokenReserve, amount*10**ethDecimals)/(10**tokenDecimals))/amount, amount] for amount in amounts]

    # def priceD(amounts):
    #     ethReserve = dai.eth_supply()
    #     tokenReserve = dai.token_supply()
    #     tokenDecimals = dai.token_data['decimals']
    #     ethDecimals = 18
    #     price = (tokenReserve/10**tokenDecimals)/(ethReserve/10**ethDecimals)
    #     return [[amount*price/(inputPrice(tokenReserve, ethReserve, price*amount*10**tokenDecimals)/(10**ethDecimals)), amount] for amount in amounts]

    '''MARKET'''
    def price(self, convert=True, block=None):
        if convert:
            eth = self.eth_supply(unit='ether', block=block)
            token = self.token_supply(factor=self.token_data['decimals'], block=block)
        else:
            eth = self.eth_supply(block=block)
            token = self.token_supply(block=block)
        return token/eth

    '''EVENTS'''
    # def purchaseEvent(self):
    #     eth = mkr.historical_event('EthPurchase')
    #     eth['tokens'] = -1*eth['tokens_sold'].astype(float)/(10**mkr.token_data['decimals'])
    #     eth['eth'] = eth['eth_bought'].astype(float)/(10**18)
    #     eth['price'] = eth['tokens']/eth['eth']
    #     eth.drop(columns=['tokens_sold', 'eth_bought'])

    def historical_event(self, event):
        exchange = self.token_data['symbol']
        temp = pd.read_csv('../SilentRed/uniswap/data/exchanges/' + exchange + '/' + event + '.csv', index_col=0)
        return temp.drop_duplicates()

    def historical_events(self):
        other = pd.DataFrame()
        for event in self.event_mapping:
            temp = self.historical_event(event)
            other = pd.concat((other, temp), ignore_index=True, sort=False)
        return other

    def _support_exchange(self, address):
        symbol = self.parent.tokens[address]
        os.mkdir('../SilentRed/uniswap/data/exchanges/' + symbol)
        for event in self.event_mapping:
            self._collect_event(event, save=True)

    def _ensure_types(self, one, two):
        temp = one.dtypes[one.dtypes.sort_index() != two.dtypes.sort_index()]
        new = two.copy()
        columns, types = list(temp.index), list(temp.astype(str).values)
        if columns:
            for selection in zip(columns, types):
                new[selection[0]] = two[selection[0]].astype(selection[1])
        return one, new

    def _collect_event(self, event, save=False, stride=2000, all=False):
        if all:
            block = self.get_earliest_block
        else:
            filepath = '../SilentRed/uniswap/data/exchanges/' + self.token_data['symbol'] + '/' + event + '.csv'
            old = pd.read_csv(filepath, index_col=0)
            block = int(old['blockNumber'].max() - 2)
        logs_gen = self.get_events(event, fromBlock=block, toBlock=self.block, stride=stride)
        fields = self.event_mapping[event]['fields']
        other = pd.DataFrame()
        for item in logs_gen:
            temp = pd.DataFrame(item)
            other = pd.concat((other, temp), ignore_index=True)
        if not other.empty:
            df = other[fields]
            filepath = '../SilentRed/uniswap/data/exchanges/' + self.token_data['symbol'] + '/' + event + '.csv'
            if save and all:
                df.to_csv(filepath)
            if save and not all:
                old, df = self._ensure_types(old, df)
                new = self._merge(old, df, 'blockNumber')
                print('Event -', event, '- Old: ', old.shape, ' New: ', df.shape, ' Merged: ', new.shape)
                new.to_csv(filepath)
            else:
                return df
        else:
            print('Empty!')
            return temp

    def _collect_events(self, save=False, stride=2000, all=False):
        for event in list(self.event_mapping.keys()):
            self._collect_event(event, save=True)

    '''ADDRESSES'''
    def event_addresses(self, event):
        events = self.historical_event(event)
        entities = self.event_mapping[event]['entity']
        addresses = dict()
        for entity in entities:
            addresses[entity] = set(events[entity].values)
        return addresses

    def events_addresses(self, events, compact=False):
        if compact:
            temp = set()
            for event in events:
                addresses = self.event_addresses(event)
                for item in addresses.values():
                    temp.update(item)
        else:
            temp = dict()
            for event in events:
                temp[event] = self.event_addresses(event)
        return temp

    def exchange_addresses(self, compact=False):
        return self.events_addresses(self.event_mapping.keys(), compact=compact)





'''OLD'''
# class UniswapOld(Event, Function):
#     def __init__(self, address='Factory', abi_path=None, abi=False, paid=False, update=False, local=False, ws=True):
#         self.temp_logs = None
#         self._contract_addresses = {
#             'Factory':'0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95'
#         }
#         Event.__init__(self,
#             contract_address=self._contract_addresses[address],
#             abi_path=abi_path if abi_path else f'uniswap/{address}.json',
#             abi=abi,
#             paid=paid,
#             log=True,
#             local=local,
#             ws=ws
#             )
#         self.tokens = {}
#         with open(os.path.join(os.path.dirname(__file__), 'Exchanges.json'), 'r') as f:
#             self.exchanges = json.load(f)
#         with open(os.path.join(os.path.dirname(__file__), 'Tokens.json'), 'r') as f:
#             self.tokens = json.load(f)

#     def active_addresses(self):
#         pass

#     def add_coin_data(self, token, prefix=''):
#         if prefix:
#             if token not in self.tokens:
#                 self.tokens[token] = {}
#             if prefix not in self.tokens[token]:
#                 data = Token(token).data(prefix=prefix)
#                 self.tokens[token][prefix] = data
#             return self.tokens[token][prefix]
#         else:
#             if token not in self.tokens:
#                 data = Token(token).data()
#                 self.tokens[token] = data
#             return self.tokens[token]

#     @staticmethod
#     def filter_exchange(item, wanted = {'token', 'symbol', 'name', 'decimals', 'exchange'}):
#         filter_ = lambda x: {k:x[k] for k in x if k in wanted}
#         return filter_(item)

#     def update_exchange(self, log):
#         try:
#             coin = self.add_coin_data(log['token'])
#             log.update(coin)
#             filtered = self.filter_exchange(log)
#             data = {filtered['token']:filtered}
#             print('  Token Passed: {}'.format(filtered['token']))
#             self.exchanges['address'].update(data)
#             data = {filtered['symbol']:filtered}
#             print('  Symbol Passed: {}'.format(filtered['token']))
#             self.exchanges['symbol'].update(data)
#             self.exchanges['updated'] = log['blockNumber']
#             self.exchanges['count'] = len(self.exchanges['address'].keys())
#             print('Added {}'.format(filtered['symbol']))
#             return True
#         except Exception as e:
#             print('Lossed ', log['token'], ': ', e)
#             return False

#     def update(self):
#         target = self.exchanges['updated']
#         end = self.block
#         if not self.temp_logs:
#             self.temp_logs = self.get_events('NewExchange', fromBlock = target, stride = 1000)
#         passed = 0
#         for log in self.temp_logs:
#             item = self.update_exchange(log)
#             if item:
#                 passed += 1
#         self._update_cold()
#         print('added {} new from block {} to {}'.format(passed, target, end - 1))

#     def _update_cold(self):
#         with open(os.path.join(os.path.dirname(__file__), 'Exchanges.json'), 'w') as f:
#             json.dump(self.exchanges, f)
#         with open(os.path.join(os.path.dirname(__file__), 'Tokens.json'), 'w') as f:
#             json.dump(self.tokens, f)

# class UniswapExchangeOld(Event, Function):
#     def __init__(self, address=None, abi_path=None, abi=False, paid=False, update=False, log=True, local=False, ws=True):
#         with open(os.path.join(os.path.dirname(__file__), 'Exchanges.json'), 'r') as f:
#             self.exchanges = json.load(f)
#         addresses = self.exchanges['address']
#         if address in addresses.keys():
#             exchange_address = addresses[address]['exchange']
#             self.token_address = address
#         else:
#             exchanges = {item[1]['exchange']:item[1] for item in addresses.items()}
#             if address in exchanges.keys():
#                 exchange_address = address
#                 self.token_address = exchanges[address]['token']
#             else:
#                 raise ValueError('Address not found in ERC20 addresses or Uniswap Exchange Addresses.')
#         Event.__init__(self,
#             contract_address=exchange_address,
#             abi_path='uniswap/Exchange.json',
#             abi=abi,
#             paid=paid,
#             log=log,
#             local=local,
#             ws=ws
#             )
#         self.token = Token(self.token_address)
#         self.token_data = self.token.data()

#     def token_balance(self, address, block=None, factor=0, unit=None):
#         if not block:
#             block = self.block
#         balance = self.token.contract.functions.balanceOf(self.contract_address).call(block)
#         if unit:
#             return self.conn.fromWei(balance, unit)
#         else:
#             return balance/10**factor

        
