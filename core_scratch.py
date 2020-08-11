from .w3 import Contract
import pandas as pd

class Event(Contract):
    def __init__(self, contract_address, abi_path, abi, paid, log, local, ws):
        Contract.__init__(self, contract_address, abi_path, abi, paid, log, local, ws)

    def _events_gen(self, event_obj, fromBlock, toBlock, time=False):
        events = event_obj.getLogs(fromBlock=fromBlock, toBlock=toBlock)
        if events:
            for item in events:
                # Protocol agnostic processing
                item.__dict__.update(item.args)
                item.__dict__.pop('args')
                if time:
                    item.__dict__.update({'timestamp':pd.datetime.fromtimestamp(
                        self.conn.eth.getBlock(item.blockNumber).timestamp)})
                # Yield item
                yield item.__dict__

    def get_events(self, name, fromBlock=None, toBlock=None, stride=1000):
        '''Yields generator'''
        event_obj = getattr(self.contract.events, name)
        if fromBlock is None:
            fromBlock = self.block - 126
        if toBlock is None:
            toBlock = self.block
        block = fromBlock
        while block < toBlock:
            if block + stride < toBlock:
                end = block + stride
            else:
                end = toBlock
            yield (item for item in self._events_gen(event_obj, block, end))
            block += stride


    def get_events2(self, name, fromBlock=None, toBlock=None, stride=1000):
        '''Yields generator'''
        event_obj = getattr(self.contract.events, name)
        if fromBlock is None:
            fromBlock = self.block - 126
        if toBlock is None:
            toBlock = self.block
        block = fromBlock
        while block < toBlock:
            if block + stride < toBlock:
                end = block + stride
            else:
                end = toBlock
            yield from self._events_gen(event_obj, block, end)
            block += stride

from .w3 import Contract

class Function(Contract):
    def __init__(self, contract_address, abi_path, abi, paid):
        Contract.__init__(self, contract_address, abi_path, abi, paid)

from .w3 import Contract
from web3.exceptions import InsufficientData, BadFunctionCallOutput

class Token(Contract):
    def __init__(self, contract_address, abi_path='utils/ERC20.json', abi=False, paid=True, log=False, local=False, ws=False):
        Contract.__init__(self, contract_address, abi_path, abi, paid, log, local, ws)

    def data(self, prefix=''):
        ''' Need Archival with block ID for total supply call.
        #temp[f'{prefix.lower()}TotalSupply' if prefix else 'totalSupply'] = self.contract.functions.totalSupply().call()
        '''
        temp = {}
        try:
            temp[f'{prefix.lower()}Symbol' if prefix else 'symbol'] = self.contract.functions.symbol().call()
        except OverflowError:
            self.__init__(self.contract_address, abi_path='utils/Bytes.json')
            obj =  self.conn.toText(self.contract.functions.symbol().call())
            temp[f'{prefix.lower()}Symbol' if prefix else 'symbol'] = repr(obj)[1:-1].split('\\')[0]
            obj = self.conn.toText(self.contract.functions.name().call())
            temp[f'{prefix.lower()}Name' if prefix else 'name'] = repr(obj)[1:-1].split('\\')[0]
            return temp
        except BadFunctionCallOutput:
            print('BFCO Lossed: ', self.contract_address)
        else:
            temp[f'{prefix.lower()}Name' if prefix else 'name'] = self.contract.functions.name().call()
        finally:
            temp[f'{prefix.lower()}Decimals' if prefix else 'decimals'] = self.contract.functions.decimals().call()
            temp[f'{prefix.lower()}TokenAddress' if prefix else 'tokenAddress'] = self.contract_address
            return temp

from web3 import Web3
import json
import os
import warnings

class w3(object):
    def __init__(self, paid=True, log=False, local=False, ws=True):
        self.paid = paid
        self.log = log
        self.local = local
        self.ws = ws
        infura_base = 'mainnet.infura.io/'
        infura_endpoint = {'paid':'xxx', 'free':'xxxx'}
        local_base = 'localhost'
        local_endpoint = {'http':':8545', 'ws':':8546'}
        ws_endpoint = 'ws/'
        v = 'v3/'
        scheme = {'https':'https://', 'http':'http://', 'wss':'wss://', 'ws':'ws://'}

        if local or not paid:
            if ws:
                self.connect(scheme['ws'], local_base, local_endpoint['ws'])
            else:
                self.connect(scheme['http'], local_base, local_endpoint['http'])
        else:
            if paid and ws:
                self.connect(scheme['wss'], infura_base, ws_endpoint, v, infura_endpoint['paid'])
            elif paid and not ws:
                self.connect(scheme['https'], infura_base, v, infura_endpoint['paid'])
            elif not paid and ws:
                self.connect(scheme['wss'], infura_base, ws_endpoint, v, infura_endpoint['free'])
            elif not paid and not ws:
                self.connect(scheme['https'], infura_base, v, infura_endpoint['free'])

    @property
    def block(self):
        return self.conn.eth.blockNumber

    def connect_ws(self):
        return Web3(Web3.WebsocketProvider(
            self.url,
            websocket_kwargs={'timeout': 180},
            websocket_timeout=180)
            )

    def connect_http(self):
        return Web3(Web3.HTTPProvider(self.url))

    def connect(self, *args):
        if 'http' in args[0]:
            self.url = ''.join(args)
            self.conn = self.connect_http()
        elif 'ws' in args[0]:
            self.url = ''.join(args)
            self.conn = self.connect_ws()
        _ = self.block
        if self.log:
            print(self.url)
            print('Paid:', self.paid, ' Local:', self.local, ' WS:', self.ws)
            print('Block: ', self.block)

    def address_balance(self, address, factor=0, unit=None):
        # SOON TO BE DEPRECATED
        warnings.warn("address_balance soon to be deprecated", DeprecationWarning)
        if unit:
            return self.conn.fromWei(self.conn.eth.getBalance(self.conn.toChecksumAddress(address)), unit)
        else:
            return self.conn.eth.getBalance(self.conn.toChecksumAddress(address))/10**factor

    def _convert(self, amount, unit=None, factor=0):
        if unit:
            return float(self.conn.fromWei(amount, unit))
        else:
            return amount/10**factor

    def eth_balance(self, address, block=None, **kwargs):
        if not block:
            block = self.block
        bal = self.conn.eth.getBalance(self.conn.toChecksumAddress(address), block_identifier=block)
        return self._convert(bal, **kwargs)



class Contract(w3):
    def __init__(self, contract_address, abi_path, abi=False, paid=True, log=False, local=False, ws=True):
        w3.__init__(self, paid, log, local, ws)
        self.contract_address = self.conn.toChecksumAddress(contract_address)
        DIR = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(DIR, '..', abi_path)
        if not abi:
            with open(path) as f:
                self.abi = json.load(f)
        else:
            self.abi = abi
        self.contract = self.conn.eth.contract(
            address=self.contract_address,
            abi=self.abi
            )
        self.functions = self.contract.functions
        self.events = self.contract.events
