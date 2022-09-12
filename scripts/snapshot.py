import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from functools import partial, wraps
from itertools import zip_longest
from pathlib import Path

import toml
from brownie import MerkleDistributor, Wei, accounts, interface, rpc, web3, Contract
from eth_abi import decode_single, encode_single
from eth_abi.packed import encode_abi_packed
from eth_utils import encode_hex
from toolz import valfilter, valmap
from tqdm import tqdm, trange
from click import secho

# DISTRIBUTION_AMOUNT = Wei('8000000 ether')
# DISTRIBUTOR_ADDRESS = '0x5e37996bcfF8C169e77b00D7b6e7261bbC60761e'
START_BLOCK = 46100093
SNAPSHOT_BLOCK = 46777595

SNAPSHOT_BLOCK = START_BLOCK + 100000


OXD_ADDRESS = "0xc5A9848b9d145965d821AaeC8fA32aaEE026492d"
SEX_ADDRESS = "0xD31Fcd1f7Ba190dBc75354046F6024A9b86014d7"

TOKENS = {
    'veNFT': '0xcBd8fEa77c2452255f59743f55A3Ea9d83b3c72b',
    'oxSOLID': '0xDA0053F0bEfCbcaC208A3f867BB243716734D809',
    'SOLID': '0x888EF71766ca594DED1F0FA3AE64eD2941740A20',
    'solidSEX': '0x41adAc6C1Ff52C5e27568f27998d747F7b69795B',
    'OXD': OXD_ADDRESS,
    'SEX': SEX_ADDRESS
}
OXD = Contract(OXD_ADDRESS)
SEX = Contract(SEX_ADDRESS)
USER_PROXY_FACTORY = Contract("0xDA00Aad945d0d5F1B1b3FBb6E0ce3E36827A7bF5")
VL_OXD_ADDRESS = "0xDA00527EDAabCe6F97D89aDb10395f719E5559b9"
VL_SEX_ADDRESS = "0xDcC208496B8fcc8E99741df8c6b8856F1ba1C71F"
VL_OXD = Contract(VL_OXD_ADDRESS)
VL_SEX = Contract(VL_SEX_ADDRESS)

BURN_ADDRESS = "0x12e569CE813d28720894c2A0FFe6bEC3CCD959b2"
BURNING_ESCROW_ADDRESS = "0x16A3a99BEe5cA47a21E6AF9B08e9EcDc56c0a339"
BURN_DELEGATOR_ADDRESS = "0x15D5823b33Ad6c272274a8Dc61E617153AB1da1D"

TOKENS = valmap(interface.ERC20, TOKENS)

def cached(path):
    path = Path(path)
    codec = {'.toml': toml, '.json': json}[path.suffix]
    codec_args = {'.json': {'indent': 2}}.get(path.suffix, {})
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if path.exists():
                print('load from cache', path)
                return codec.loads(path.read_text())
            else:
                result = func(*args, **kwargs)
                os.makedirs(path.parent, exist_ok=True)
                path.write_text(codec.dumps(result, **codec_args))
                print('write to cache', path)
                return result
        return wrapper
    return decorator


def transfers_to_balances(address):
    balances = Counter()
    contract = Contract(address)
    for start in trange(START_BLOCK, SNAPSHOT_BLOCK, 1000):
        end = min(start + 999, SNAPSHOT_BLOCK)
        
        logs = contract.events.Transfer().getLogs(fromBlock=start, toBlock=end)
        for log in logs:
            to_address = log['args']['to']
            from_address = log['args']['from']
            token_id = log['args'].get('tokenId')
            is_nft = token_id is not None
            transaction_hash = log['transactionHash'].hex()
            transaction_is_escrowed = (from_address == BURNING_ESCROW_ADDRESS and to_address == BURN_ADDRESS) or (from_address == BURN_ADDRESS and to_address == BURNING_ESCROW_ADDRESS)
            transaction_is_burn = to_address == BURN_ADDRESS or to_address == BURNING_ESCROW_ADDRESS
            transaction_is_refund = from_address == BURN_ADDRESS or from_address == BURNING_ESCROW_ADDRESS
            if transaction_is_escrowed:
                continue
            if is_nft:
                locked = contract.locked(token_id)[0]
                if transaction_is_burn or transaction_is_refund:
                    assert locked > 0, "Invalid value: " + transaction_hash
                if transaction_is_burn:
                    # Burn
                    balances[from_address] += locked
                elif transaction_is_refund:
                    # Refund
                    balances[to_address] -= locked
                    print("Processing refund for NFT:", token_id, transaction_hash)
            else:
                value = log['args'].get('value')
                if transaction_is_burn:
                    # Burn
                    balances[from_address] += value
                elif transaction_is_refund:
                    # Refund
                    print("Processing refund for:", contract.address, transaction_hash)
                    balances[to_address] -= value
    return valfilter(bool, dict(balances.most_common()))


@cached('snapshot/01-balances.toml')
def step_01():
    print('step 01. snapshot token balances.')
    balances = defaultdict(Counter)  # token -> user -> balance
    for name, address in TOKENS.items():
        print()
        print(f'processing {name}')
        balances[name] = transfers_to_balances(str(address))
        if len(balances[name]) > 0:
            assert min(balances[name].values()) >= 0, 'negative balances found'
    return balances
    
@cached('snapshot/02-vloxd.toml')
def step_02():
    print('step 02. vlOXD')
    unique_users = {}
    balances = Counter()
    for start in trange(START_BLOCK, SNAPSHOT_BLOCK, 1000):
        end = min(start + 999, SNAPSHOT_BLOCK)
        logs = OXD.events.Transfer().getLogs(fromBlock=start, toBlock=end)
        for log in logs:
            to_address = log['args']['to']
            from_address = log['args']['from']
            if to_address == VL_OXD_ADDRESS:
                unique_users[from_address] = True
    for user in unique_users.keys():
        locked = VL_OXD.lockedBalanceOf(user)
        owner = ""
        if USER_PROXY_FACTORY.isUserProxy(from_address):
            owner = interface.UserProxy(from_address).ownerAddress()
        else:
            owner = from_address
        balances[owner] = int(locked)
    return valfilter(bool, dict(balances.most_common()))

@cached('snapshot/03-vlsex.toml')
def step_03():
    print('step 03. vlSEX')
    unique_users = {}
    balances = Counter()
    for start in trange(START_BLOCK, SNAPSHOT_BLOCK, 1000):
        end = min(start + 999, SNAPSHOT_BLOCK)
        logs = SEX.events.Transfer().getLogs(fromBlock=start, toBlock=end)
        for log in logs:
            to_address = log['args']['to']
            from_address = log['args']['from']
            if to_address == VL_SEX_ADDRESS:
                unique_users[from_address] = True
    for user in unique_users.keys():
        locked = VL_SEX.userBalance(user)
        balances[from_address] = int(locked)
    return valfilter(bool, dict(balances.most_common()))
    
@cached('snapshot/04-combined.toml')
def step_04(token_balances, vloxd_balances, vlsex_balances):    
    print('step 04. aggregate data')
    token_balances['vlOxd'] = vloxd_balances
    token_balances['vlSex'] = vlsex_balances
    return token_balances
    
class MerkleTree:
    def __init__(self, elements):
        self.elements = sorted(set(web3.keccak(hexstr=el) for el in elements))
        self.layers = MerkleTree.get_layers(self.elements)

    @property
    def root(self):
        return self.layers[-1][0]

    def get_proof(self, el):
        el = web3.keccak(hexstr=el)
        idx = self.elements.index(el)
        proof = []
        for layer in self.layers:
            pair_idx = idx + 1 if idx % 2 == 0 else idx - 1
            if pair_idx < len(layer):
                proof.append(encode_hex(layer[pair_idx]))
            idx //= 2
        return proof

    @staticmethod
    def get_layers(elements):
        layers = [elements]
        while len(layers[-1]) > 1:
            layers.append(MerkleTree.get_next_layer(layers[-1]))
        return layers

    @staticmethod
    def get_next_layer(elements):
        return [MerkleTree.combined_hash(a, b) for a, b in zip_longest(elements[::2], elements[1::2])]

    @staticmethod
    def combined_hash(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return web3.keccak(b''.join(sorted([a, b])))


def calculate_merkle_tree(balances):
    elements = [(index, account, amount) for index, (account, amount) in enumerate(balances.items())]
    nodes = [encode_hex(encode_abi_packed(['uint', 'address', 'uint'], el)) for el in elements]
    tree = MerkleTree(nodes)
    distribution = {
        'merkleRoot': encode_hex(tree.root),
        'tokenTotal': hex(sum(balances.values())),
        'claims': {
            user: {'index': index, 'amount': hex(amount), 'proof': tree.get_proof(nodes[index])}
            for index, user, amount in elements
        },
    }
    print(f'merkle root: {encode_hex(tree.root)}')
    return distribution

@cached('snapshot/merkle-venft-distribution.json')
def merkle_venft(balances):
    return calculate_merkle_tree(balances)

@cached('snapshot/merkle-solid-distribution.json')
def merkle_solid(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-oxsolid-distribution.json')
def merkle_oxsolid(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-solidsex-distribution.json')
def merkle_solidsex(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-oxd-distribution.json')
def merkle_oxd(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-sex-distribution.json')
def merkle_sex(balances):
    return calculate_merkle_tree(balances)
# def deploy():
#     user = accounts[0] if rpc.is_active() else accounts.load(input('account: '))
#     tree = json.load(open('snapshot/07-merkle-distribution.json'))
#     root = tree['merkleRoot']
#     token = str(DAI)
#     MerkleDistributor.deploy(token, root, {'from': user})


# def claim():
#     claimer = accounts.load(input('Enter brownie account: '))
#     dist = MerkleDistributor.at(DISTRIBUTOR_ADDRESS)
#     tree = json.load(open('snapshot/07-merkle-distribution.json'))
#     claim_other = input('Claim for another account? y/n [default: n] ') or 'n'
#     assert claim_other in {'y', 'n'}
#     user = str(claimer) if claim_other == 'n' else input('Enter address to claim for: ')

#     if user not in tree['claims']:
#         return secho(f'{user} is not included in the distribution', fg='red')
#     claim = tree['claims'][user]
#     if dist.isClaimed(claim['index']):
#         return secho(f'{user} has already claimed', fg='yellow')

#     amount = Wei(int(claim['amount'], 16)).to('ether')
#     secho(f'Claimable amount: {amount} DAI', fg='green')
#     if claim_other == 'n':  # no tipping for others
#         secho(
#             '\nThe return of funds to you was made possible by a team of volunteers who worked for free to make this happen.'
#             '\nPlease consider tipping them a portion of your recovered funds as a way to say thank you.\n',
#             fg='yellow',
#         )
#         tip = input('Enter tip amount in percent: ')
#         tip = int(float(tip.rstrip('%')) * 100)
#         assert 0 <= tip <= 10000, 'invalid tip amount'
#     else:
#         tip = 0

#     tx = dist.claim(claim['index'], user, claim['amount'], claim['proof'], tip, {'from': claimer})
#     tx.info()


def main():
    token_balances = step_01()
    vloxd_balances = step_02()
    vlsex_balances = step_03()
    combined_balances = step_04(token_balances, vloxd_balances, vlsex_balances)
    
    merkle_venft(combined_balances['veNFT'])
    merkle_oxsolid(combined_balances['oxSOLID'])
    merkle_solidsex(combined_balances['solidSEX'])
    merkle_oxd(combined_balances['OXD'])
    merkle_sex(combined_balances['SEX'])
    merkle_oxsolid(combined_balances['SOLID'])
    